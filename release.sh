#!/usr/bin/env sh

set -e


BASE_BRANCH_NAME=""
RELEASE_BRANCH_NAME=""
CURRENT_PROJECT_VERSION=""
RELEASE_VERSION=""
NEXT_DEV_VERSION=""


# color constants filled by enable_coloring()
C_BOLD=""
C_UNDERLINE=""
C_STANDOUT=""
C_NORMAL=""
C_BLACK=""
C_RED=""
C_GREEN=""
C_YELLOW=""
C_BLUE=""
C_MAGENTA=""
C_CYAN=""
C_WHITE=""


print_usage() {
cat << EOF
Usage: $( basename "$0" ) [options]

Prepares a release by perfroming the following steps:

  - create a new branch for the release commits
  - create a commit and a tag for the new release version
  - verify that tag by invoking "./mvnw clean verify"
  - increment to the next development version and commit it
  - push the new branch to the remote repository
  - TODO automatically open a Pull Request

OPTIONS:
    -h      Show this message
EOF
}


enable_coloring() {
  # check if stdout is a terminal...
  if [ -t 1 ] ; then
    # see if it supports colors...
    NCOLORS=$( tput colors )
    if test -n "$NCOLORS" && test $NCOLORS -ge 8 ; then
      C_BOLD="$( tput bold )"
      C_UNDERLINE="$( tput smul )"
      C_STANDOUT="$( tput smso )"
      C_NORMAL="$( tput sgr0 )"
      C_BLACK="$( tput setaf 0 )"
      C_RED="$( tput setaf 1 )"
      C_GREEN="$( tput setaf 2 )"
      C_YELLOW="$( tput setaf 3 )"
      C_BLUE="$( tput setaf 4 )"
      C_MAGENTA="$( tput setaf 5 )"
      C_CYAN="$( tput setaf 6 )"
      C_WHITE="$( tput setaf 7 )"
    fi
  fi
}


confirm_action() {
  local YN

  printf "$1 (Y/n) "

  while true; do
    read YN
    if [ -n "$YN" ]; then
      case "$YN" in
          y|Y) return 0;;
          n|N) return 1;;
          *)   printf "To confirm, enter 'y', 'Y' or nothing at all, to decline, enter 'n' or 'N': ";;
      esac
    else
      return 0
    fi
  done
}


ensure_working_copy_is_clean() {
  printf "%s" "${C_YELLOW}Working directory state: ${C_NORMAL}"

  if [ -n "$( git status --porcelain )" ]; then
    echo "${C_RED}dirty${C_NORMAL}"
    git status
    return 1
  fi

  echo "${C_GREEN}clean${C_NORMAL}"
}


git_get_current_branch_name() {
  git rev-parse --symbolic-full-name --abbrev-ref HEAD
}


git_get_upstream_remote_for_local_branch() {
  local BASE_BRANCH_FULL_REF UPSTREAM_REMOTE_REF UPSTREAM_REMOTE_BRANCH

  BASE_BRANCH_FULL_REF=$( git rev-parse --symbolic-full-name "$1" )
  UPSTREAM_REMOTE_REF=$( git for-each-ref --format='%(upstream)' "$BASE_BRANCH_FULL_REF" )

  case "$UPSTREAM_REMOTE_REF" in
    refs/remotes/*)
      UPSTREAM_REMOTE_BRANCH=${UPSTREAM_REMOTE_REF##"refs/remotes/"}
      echo ${UPSTREAM_REMOTE_BRANCH%/*}
      ;;
  esac
}


maven_get_project_version() {
  # see https://stackoverflow.com/a/26514030
  ./mvnw                                  \
      -Dexec.executable="echo"         \
      -Dexec.args='${project.version}' \
      --quiet                          \
      --non-recursive                  \
      --batch-mode                     \
      org.codehaus.mojo:exec-maven-plugin:1.5.0:exec 2>&-
}


maven_set_version() {
  ./mvnw -DnewVersion="$1" -DgenerateBackupPoms="false" --batch-mode versions:set
}

version_string_complies_to_standards() {
  if ( echo "$1" | grep -q -E '^[0-9]+\.[0-9]+\.[0-9]+(-SNAPSHOT)?$' ); then
    return 0
  else
    return 1
  fi
}


get_version_number() {
  echo "$1" | cut -d '-' -f1
}


get_major_version_number() {
  echo "$1" | cut -d '.' -f1
}


get_minor_version_number() {
  echo "$1" | cut -d '.' -f2
}


get_patch_version_number() {
  echo "$1" | cut -d '.' -f3  | sed -E 's/^([0-9]+)([^0-9].*)?/\1/g'
}


advance_version_number() {
  local MAJOR MINOR PATCH

  MAJOR=$( get_major_version_number "$1" )
  MINOR=$( get_minor_version_number "$1" )
  PATCH=$( get_patch_version_number "$1" )
  PATCH=$(($PATCH+1))

  echo "${MAJOR}.${MINOR}.${PATCH}-SNAPSHOT"
}


determine_git_base_branch() {
  printf "%s" "${C_YELLOW}Base git branch:         ${C_NORMAL}"
  BASE_BRANCH_NAME="$( git_get_current_branch_name )"
  echo "${C_WHITE}${BASE_BRANCH_NAME}${C_NORMAL}"
}


determine_upstream_git_remote() {
  printf "%s" "${C_YELLOW}Upstream git remote:     ${C_NORMAL}"
  UPSTREAM_REMOTE=$( git_get_upstream_remote_for_local_branch "$BASE_BRANCH_NAME" )
  if [ -n "$UPSTREAM_REMOTE" ]; then
    echo "${C_WHITE}${UPSTREAM_REMOTE}${C_NORMAL}"
  else
    echo "${C_RED}(N/A)${C_NORMAL}"
  fi
}

# Triggers the download of the Maven wrapper.
#
# The download is executed using a Java program, which writes all its output
# to stdout interfering with output from Maven itself.
download_maven_wrapper() {
  ./mvnw --version > /dev/null
}


determine_current_project_version() {
  printf "%s" "${C_YELLOW}Current project version: ${C_NORMAL}"

  CURRENT_PROJECT_VERSION=$( maven_get_project_version )
  if ! version_string_complies_to_standards "$CURRENT_PROJECT_VERSION"; then
    echo "${C_WHITE}${CURRENT_PROJECT_VERSION} ${C_RED}(unsupported format)${C_NORMAL}"
    return 1
  fi

  echo "${C_GREEN}${CURRENT_PROJECT_VERSION}${C_NORMAL}"
}


read_next_dev_version() {

  if [ -z "$NEXT_DEV_VERSION" ]; then
    NEXT_DEV_VERSION=$( advance_version_number "$CURRENT_PROJECT_VERSION" || echo "" )
  fi

  while true; do
    if [ -z "$NEXT_DEV_VERSION" ]; then
      printf "%s" "Please enter the next development version (e.g. 0.42.5-SNAPSHOT): "
      read NEXT_DEV_VERSION
    fi
    if [ -n "$NEXT_DEV_VERSION" ]; then
      if ! version_string_complies_to_standards "$NEXT_DEV_VERSION"; then
        echo "${C_RED}Version doesn't comply to standards: ${C_WHITE}${NEXT_DEV_VERSION}${C_NORMAL}"
      elif confirm_action "Use ${C_BLUE}${NEXT_DEV_VERSION}${C_NORMAL} as next development version?"; then
        return
      fi
      NEXT_DEV_VERSION=""
    fi
  done

}


read_release_branch_name() {
  local VERSION_NUMBER

  if [ -z "$RELEASE_BRANCH_NAME" ]; then
    if version_string_complies_to_standards "$CURRENT_PROJECT_VERSION"; then
      VERSION_NUMBER=$( get_version_number "$CURRENT_PROJECT_VERSION" )
      RELEASE_BRANCH_NAME="release-${VERSION_NUMBER}"
    fi
  fi

  while true; do
    if [ -z "$RELEASE_BRANCH_NAME" ]; then
      printf "%s" "Please enter the name of the new release branch (e.g. release-0.42.4): "
      read RELEASE_BRANCH_NAME
    fi
    if [ -n "$RELEASE_BRANCH_NAME" ]; then
      if git show-ref --quiet --verify -- "refs/heads/$RELEASE_BRANCH_NAME"; then
        echo "${C_RED}Branch already exists in local repository: ${C_WHITE}${RELEASE_BRANCH_NAME}${C_NORMAL}"
      elif confirm_action "Use ${C_BLUE}${RELEASE_BRANCH_NAME}${C_NORMAL} as new release branch name?"; then
        return
      fi
      RELEASE_BRANCH_NAME=""
    fi
  done

}


read_release_version() {

  if [ -z "$RELEASE_VERSION" ]; then
    RELEASE_VERSION=$( get_version_number "$CURRENT_PROJECT_VERSION" )
  fi

  while true; do
    if [ -z "$RELEASE_VERSION" ]; then
      printf "%s" "Please enter the next release version (e.g. 0.42.4): "
      read RELEASE_VERSION
    fi
    if [ -n "$RELEASE_VERSION" ]; then
      if ! version_string_complies_to_standards "$RELEASE_VERSION"; then
        echo "${C_RED}Version doesn't comply to standards: ${C_WHITE}${RELEASE_VERSION}${C_NORMAL}"
      elif git show-ref --quiet --verify -- "refs/tags/$RELEASE_VERSION"; then
        echo "${C_RED}Tag already exists in local repository: ${C_WHITE}${RELEASE_VERSION}${C_NORMAL}"
      elif confirm_action "Use ${C_BLUE}${RELEASE_VERSION}${C_NORMAL} as new release version?"; then
        return
      fi
      RELEASE_VERSION=""
    fi
  done
}


create_release_branch() {
  git checkout -b "$RELEASE_BRANCH_NAME"
}


create_release_commit_and_tag() {
  maven_set_version "$RELEASE_VERSION"
  git commit --message="Release ${RELEASE_VERSION}" -- pom.xml
  git tag --sign --message="Release ${RELEASE_VERSION}" -- "${RELEASE_VERSION}"
  git checkout "${RELEASE_VERSION}"
  ./mvnw -DperformRelease --batch-mode clean verify
}


advance_to_next_dev_version() {
  git checkout "${RELEASE_BRANCH_NAME}"
  maven_set_version "$NEXT_DEV_VERSION"
  git commit --message="Release ${RELEASE_VERSION} - next development version is ${NEXT_DEV_VERSION}" -- pom.xml
}


push_release_branch_to_upstream_remote() {
  if [ -n "$UPSTREAM_REMOTE" ]; then
    if confirm_action "Push branch ${C_BLUE}${RELEASE_BRANCH_NAME}${C_NORMAL} to remote ${C_BLUE}${UPSTREAM_REMOTE}${C_NORMAL}?"; then
      git push --set-upstream "${UPSTREAM_REMOTE}" "${RELEASE_BRANCH_NAME}"
    fi
  fi
}


do_release() {

  ensure_working_copy_is_clean
  determine_git_base_branch
  determine_upstream_git_remote
  download_maven_wrapper
  determine_current_project_version
  read_release_version
  read_next_dev_version
  read_release_branch_name
  create_release_branch
  create_release_commit_and_tag
  advance_to_next_dev_version
  push_release_branch_to_upstream_remote

}


OPTIND=1
while getopts “h” OPTION
do
  case $OPTION in
    h)  print_usage; exit;;
    \?) print_usage; exit 1;;
  esac
done
shift $(($OPTIND - 1))

enable_coloring
do_release
