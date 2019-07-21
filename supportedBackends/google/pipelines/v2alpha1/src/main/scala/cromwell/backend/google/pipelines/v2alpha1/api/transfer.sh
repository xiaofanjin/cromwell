#!/usr/bin/env bash

gsutil_log="gsutil_output.txt"


localize_file() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  echo -n "rm -f $HOME/.config/gcloud/gce && gsutil ${rpflag} -m cp '$cloud' '$container' 2>&1 > '$gsutil_log'"
}

localize_directory() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  local dir=$(dirname "${container}")
  echo -n "mkdir -p '${dir}' && rm -f $HOME/.config/gcloud/gce && gsutil ${rpflag} -m rsync -r '${cloud}' '${container}'"
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
  echo -n "rm -f $HOME/.config/gcloud/gce && gsutil ${rpflag} -m ${content_flag} cp '$container' '$cloud'"
}

delocalize_directory() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  local content="$4"

  local content_flag=$(gsutil_content_flag $content)
  echo -n "rm -f $HOME/.config/gcloud/gce && gsutil ${rpflag} -m ${content_flag} rsync -r '$container' '$cloud'"
}

timestamped_message() {
  printf '%s %s\n' "$(date -u '+%Y/%m/%d %H:%M:%S')" "$1"
}

localize_message() {
  local cloud="$1"
  local container="$2"
  local message=$(printf "Localizing input %s -> %s" "$cloud" "$container")
  $(timestamped_message "${message}")
}

delocalize_message() {
  local cloud="$1"
  local container="$2"
  local message=$(printf "Delocalizing output %s -> %s" "$container" "$cloud")
  $(timestamped_message "${message}")
}

# Transfer a bundle of files xor directories to or from the same GCS bucket.
transfer() {
  local direction="$1"
  local file_or_directory="$2"
  local project="$3"
  local max_attempts="$4"

  if [[ "$direction" != "localize" && "$direction" != "delocalize" ]]; then
    echo "direction must be 'localize' or 'delocalize' but got '$direction'"
    exit 1
  fi

  if [[ "$file_or_directory" != "file" -a "$file_or_directory" != "directory" ]]; then
    echo "file_or_directory must be 'file' or 'directory' but got '$file_or_directory'"
    exit 1
  fi

  shift; shift; shift; shift # direction; file_or_directory; project; max_attempts

  # Whether the requester pays status of the GCS bucket is certain. rp status is presumed false until proven otherwise.
  local rp_status_certain=false
  local use_requester_pays=false

  local message_fn="${direction}_message"

  # One race-condition sidestepping sleep 5 to rule them all.
  sleep 5

  # Loop while there are still files or directories to localize or delocalize.
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
    $(${message_fn} "$cloud" "$container")

    if [[ ${use_requester_pays} = true ]]; then
      rpflag="-u ${project}"
    else
      rpflag=""
    fi

    transfer_fn="${direction}_${file_or_directory}"
    # Note that the file versions of transfer functions will be passed a content_type parameter that
    # they won't use.
    command=$(${transfer_fn} "$cloud" "$container" "$rpflag" "$content_type")

    attempt=0
    # Loop attempting transfers for this file or directory while attempts are not exhausted.
    while [[ ${attempt} -le ${max_attempts} ]]; do
      # Run the transfer command.
      $(${command})

      if [[ $? = 0 ]]; then
        rp_status_certain=true
        break
      else
        $(timestamped_message "${command} failed")
        # Print the reason of the failure.
        cat "${gsutil_log}"

        # If the requester pays status of the GCS bucket is not certain look for requester pays errors.
        if [[ ${rp_status_certain} = false ]]; then
          if grep -q "Bucket is requester pays bucket but no user project provided." "${gsutil_log}"; then
            $(timestamped_message "Retrying with user project")
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
      exit $?
    fi
  done

}


# Scala code will write these bundles. One bundle per source/target GCS bucket since requester pays status
# is specific to individual GCS buckets.

localize_files_bundle=(
  "localize"   # direction
  "file"       # file or directory
  "project1"   # project
  "3"          # max transfer attempts
  "cloud1"     # cloud path 1
  "container1" # container path 1
  "cloud2"     # cloud path 2
  "container2" # container path 2
)
transfer "${localize_files_bundle[@]}"

localize_directories_bundle=(
  "localize"
  "directory"
  "project2"
  "3"
  "cloud1"
  "container1"
  "cloud2"
  "container2"
)
transfer "${localize_directories_bundle[@]}"

delocalize_directories_bundle=(
  "delocalize"
  "directory"
  "project3"
  "3"
  "cloud1"
  "container1"
  "content_type1"
  "cloud2"
  "container2"
  "content_type2"
)
