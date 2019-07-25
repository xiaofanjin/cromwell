# The `papi_v2_log` Centaur test is opinionated about the number of log messages around localization/delocalization.
# The trace logging of `set -x` must be turned off for the `papi_v2_log` test to pass.
set +x

gsutil_log="gsutil_output.txt"


localize_file() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  # Do not quote rpflag, when that is set it will be -u project which should be two distinct arguments.
  rm -f "$HOME/.config/gcloud/gce" && gsutil ${rpflag} -m cp "$cloud" "$container" > "$gsutil_log" 2>&1
}

localize_directory() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  # Do not quote rpflag, when that is set it will be -u project which should be two distinct arguments.
  mkdir -p "${container}" && rm -f "$HOME/.config/gcloud/gce" && gsutil ${rpflag} -m rsync -r "${cloud}" "${container}" > "$gsutil_log" 2>&1
}

# Content type is sometimes (not always) specified for delocalizations.
gsutil_content_flag() {
  local content="$1"

  if [[ ! -z "${content}" ]]; then
    echo -n -- "-h 'Content-Type: $content'"
  fi
}

delocalize_file() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  local content="$4"

  local content_flag=$(gsutil_content_flag $content)
  # Do not quote rpflag or content_flag, when those are set they will be two distinct arguments each.
  rm -f "$HOME/.config/gcloud/gce" && gsutil ${rpflag} -m ${content_flag} cp "$container" "$cloud" > "$gsutil_log" 2>&1
}

delocalize_directory() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  local content="$4"

  local content_flag=$(gsutil_content_flag $content)
  # Do not quote rpflag or content_flag, when those are set they will be two distinct arguments each.
  rm -f "$HOME/.config/gcloud/gce" && gsutil ${rpflag} -m ${content_flag} rsync -r "$container" "$cloud" > "$gsutil_log" 2>&1
}

timestamped_message() {
  printf '%s %s\n' "$(date -u '+%Y/%m/%d %H:%M:%S')" "$1"
}

localize_message() {
  local cloud="$1"
  local container="$2"
  local message=$(printf "Localizing input %s -> %s" "$cloud" "$container")
  timestamped_message "${message}"
}

delocalize_message() {
  local cloud="$1"
  local container="$2"
  local message=$(printf "Delocalizing output %s -> %s" "$container" "$cloud")
  timestamped_message "${message}"
}

# Transfer a bundle of files xor directories to or from the same GCS bucket.
transfer() {
  local direction="$1"
  local files_or_directories="$2"
  local project="$3"
  local max_attempts="$4"

  shift; shift; shift; shift # direction; files_or_directories; project; max_attempts

  if [[ "$direction" != "localize" && "$direction" != "delocalize" ]]; then
    echo "direction must be 'localize' or 'delocalize' but got '$direction'"
    exit 1
  fi

  transfer_fn_name=""
  if [[ "$files_or_directories" = "files" ]]; then
    transfer_fn_name="${direction}_file"
  elif [[ "$files_or_directories" = "directories" ]]; then
    transfer_fn_name="${direction}_directory"
  else
    echo "files_or_directories must be 'files' or 'directories' but got '$files_or_directories'"
    exit 1
  fi

  # Whether the requester pays status of the GCS bucket is certain. rp status is presumed false until proven otherwise.
  local rp_status_certain=false
  local use_requester_pays=false

  local message_fn="${direction}_message"

  # One race-condition sidestepping sleep 5 to rule them all.
  sleep 5

  # Loop while there are still files or directories in the bundle to transfer.
  while [[ $# -gt 0 ]]; do
    cloud="$1"
    container="$2"

    content_type=""
    if [[ "${direction}" = "delocalize" ]]; then
      # Content type only appears in delocalization bundles.
      content_type="$3"
      shift # content_type
    fi
    shift; shift # cloud; container

    # Log what is being localized or delocalized (at least one test depends on this).
    ${message_fn} "$cloud" "$container"

    attempt=1
    transfer_rc=0
    # Loop attempting transfers for this file or directory while attempts are not exhausted.
    while [[ ${attempt} -le ${max_attempts} ]]; do

      if [[ ${use_requester_pays} = true ]]; then
        rpflag="-u ${project}"
      else
        rpflag=""
      fi

      # Note the localization versions of transfer functions are passed a content_type parameter they will not use.
      ${transfer_fn_name} "$cloud" "$container" "$rpflag" "$content_type"
      transfer_rc=$?

      if [[ ${transfer_rc} = 0 ]]; then
        rp_status_certain=true
        break
      else
        timestamped_message "${transfer_fn_name} \"$cloud\" \"$container\" \"$rpflag\" \"$content_type\" failed"

        # Print the reason of the failure.
        cat "${gsutil_log}"

        # If the requester pays status of the GCS bucket is not certain look for requester pays errors.
        if [[ ${rp_status_certain} = false ]]; then
          if grep -q "Bucket is requester pays bucket but no user project provided." "${gsutil_log}"; then
            timestamped_message "Retrying with user project"
            use_requester_pays=true
            # Do not increment the attempt number, a requester pays failure does not count against retries.
            # Do mark that the bucket in question is certain to have requester pays status.
            rp_status_certain=true
          else
            # Requester pays status is not certain but this transfer failed for non-requester pays reasons.
            # Increment the attempt number.
            attempt=$((attempt+1))
          fi
        else
          attempt=$((attempt+1))
        fi
      fi
    done
    if [[ ${attempt} -gt ${max_attempts} ]]; then # out of attempts
      exit ${transfer_rc}
    fi
  done
}
