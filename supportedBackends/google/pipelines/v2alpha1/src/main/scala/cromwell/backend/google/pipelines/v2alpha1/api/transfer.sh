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
  echo -n "rm -f $HOME/.config/gcloud/gce && gsutil ${rpflag} ${content_flag} cp '$container' '$cloud'"
}

delocalize_directory() {
  local cloud="$1"
  local container="$2"
  local rpflag="$3"
  local content="$4"

  local content_flag=$(gsutil_content_flag $content)
  echo -n "rm -f $HOME/.config/gcloud/gce && gsutil ${rpflag} ${content_flag} -m rsync -r '$container' '$cloud'"
}

delocalize_no_requester_pays_directory() {
  local cloud="$1"
  local container="$2"
  echo -n "insert the right code here"
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

transfer() {
  local direction="$1"
  local file_or_directory="$2"
  local project="$3"

  if [[ "$direction" != "localize" && "$direction" != "delocalize" ]]; then
    echo "direction must be 'localize' or 'delocalize' but got '$direction'"
    exit 1
  fi

  if [[ "$file_or_directory" != "file" -a "$file_or_directory" != "directory" ]]; then
    echo "file_or_directory must be 'file' or 'directory' but got '$file_or_directory'"
    exit 1
  fi

  shift; shift; shift

  local max_retries=3
  local use_requester_pays=false
  local first_file=true

  local message_fn="${direction}_message"

  # One sleep 5 to rule them all
  sleep 5

  # While there are still files or directories to localize or delocalize
  while [[ $# -gt 0 ]]; do
    i=0
    cloud="$1"
    container="$2"

    content_type=""
    if [[ "${direction}" = "delocalize" ]]; then
      # content type only appears in delocalize bundles
      content_type="$3"
      shift
    fi
    shift; shift

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

    # Loop retrying transfers for this file or directory while retries are not exhausted.
    while [[ ${i} -lt ${max_retries} ]]; do
      # Run the transfer command.
      $(${command})

      if [[ $? = 0 ]]; then
        first_file=false
        break
      else
        # If this is the first file or directory in the bundle then look for rp errors, otherwise don't bother.
        if [[ ${first_file} = true ]]; then
          if grep -q "Bucket is requester pays bucket but no user project provided." "${gsutil_log}"; then
            $(timestamped_message "${command} failed")
            # Print the reason of the failure
            cat "${gsutil_log}"
            $(timestamped_message "Retrying with user project")
            use_requester_pays=true
            # Do not increment the attempt number, a requester pays failure does not count against retries.
          fi
        else
          i=$((i+1))
        fi
        first_file=false
      fi
    done
    if [[ ${i} = ${max_retries} ]]; then # out of retries
      exit $?
    fi
  done

}


# Scala code should write these bundles

localize_files_bundle=(
  "localize"   # direction
  "file"       # file or directory
  "project1"   # project
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
  "cloud1"
  "container1"
  "content_type1"
  "cloud2"
  "container2"
  "content_type2"

)
