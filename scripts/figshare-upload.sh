#!/bin/bash
#
# Upload large file to a Figshare item.
# Copied mostly from Figshare bash script example.
#

# exit script if any command fails
set -e

echo "checking ${HOME}/.config/figshare/credentials"
if [ -f "${HOME}/.config/figshare/credentials" ]; then
  echo "reading ${HOME}/.config/figshare/credentials"
  source "${HOME}/.config/figshare/credentials"
fi

echo "checking .config"
if [ -f .config ]; then
  source .config
fi

if [ -z "$FIGSHARE_ACCESS_TOKEN" ]; then
  echo 'FIGSHARE_ACCESS_TOKEN required. Please set it via env variable or .config.'
  exit 2
fi

ACCESS_TOKEN=$FIGSHARE_ACCESS_TOKEN

FILE_PATH="$1"
ITEM_ID=${2:-$FIGSHARE_ITEM_ID}
PART_PREFIX="${3:-$FIGSHARE_PART_PREFIX}"

if [ -z "$FILE_PATH" ]; then
  echo "Usage: $0 <file path> [<item id>] [<part prefix>]"
  echo ''
  echo 'A new item will be created if item id is not provided.'
  echo ''
  RESPONSE=$(curl -s -H 'Authorization: token '$ACCESS_TOKEN -X GET "https://api.figshare.com/v2/account/articles")
  echo "The item list dict contains: "$RESPONSE
  echo ''
  exit 2
fi

FILE_NAME=$(basename "$FILE_PATH")

if [ -z "${PART_PREFIX}" ]; then
  PART_PREFIX="$(dirname "$FILE_PATH")/part_"
fi

echo "PART_PREFIX=${PART_PREFIX}"

if ls "${PART_PREFIX}"* 1> /dev/null 2>&1; then
  echo "existing part files found, please review and remove them first (prefix: ${PART_PREFIX})"
  exit 2
fi

# ####################################################################################

#Retrieve the file size and MD5 values for the item which needs to be uploaded
FILE_SIZE=$(stat -c%s $FILE_PATH)

echo "Calculating MD5"
MD5=($(pv $FILE_PATH | md5sum))

echo "Uploading $FILE_PATH ($FILE_SIZE bytes, md5: $MD5) to item id $ITEM_ID"

if [ -z "$ITEM_ID" ]; then
  # Create a new item
  echo 'Creating a new item...'
  TITLE="$FILE_NAME"
  ITEM_JSON="{\"title\": \"$TITLE\"}"
  RESPONSE=$(curl -s -d "$ITEM_JSON" -H 'Authorization: token '$ACCESS_TOKEN -H 'Content-Type: application/json' -X POST "https://api.figshare.com/v2/account/articles")
  echo "The location of the created item is "$RESPONSE
  echo ''

  # Retrieve item id
  echo 'Retrieving the item id...'
  ITEM_ID=$(echo "$RESPONSE" | sed -r "s/.*\/([0-9]+).*/\1/")
  echo "The item id is "$ITEM_ID
  echo ''
fi

# List item files
echo 'Retrieving the item files...'
FILES_LIST=$(curl -s -H 'Authorization: token '$ACCESS_TOKEN -X GET "https://api.figshare.com/v2/account/articles/$ITEM_ID/files")
echo 'The files list of the newly-create item should be an empty one. Returned results: '$FILES_LIST
echo ''

# Initiate new upload:
echo 'A new upload had been initiated...'
RESPONSE=$(curl -s -d '{"md5": "'${MD5}'", "name": "'${FILE_NAME}'", "size": '${FILE_SIZE}'}' -H 'Content-Type: application/json' -H 'Authorization: token '$ACCESS_TOKEN -X POST "https://api.figshare.com/v2/account/articles/$ITEM_ID/files")
echo $RESPONSE
echo ''

# Retrieve file id
echo 'The file id is retrieved...'
FILE_ID=$(echo "$RESPONSE" | sed -r "s/.*\/([0-9]+).*/\1/")
echo 'The file id is: '$FILE_ID
echo ''

# Retrieve the upload url
echo 'Retrieving the upload URL...'
RESPONSE=$(curl -s -H 'Authorization: token '$ACCESS_TOKEN -X GET "https://api.figshare.com/v2/account/articles/$ITEM_ID/files/$FILE_ID")
UPLOAD_URL=$(echo "$RESPONSE" | sed -r 's/.*"upload_url":\s"([^"]+)".*/\1/')
echo 'The upload URL is: '$UPLOAD_URL
echo ''

# Retrieve the upload parts
echo 'Retrieving the part value...'
RESPONSE=$(curl -s -H 'Authorization: token '$ACCESS_TOKEN -X GET "$UPLOAD_URL")
PARTS_SIZE=$(echo "$RESPONSE" | sed -r 's/"endOffset":([0-9]+).*/\1/' | sed -r 's/.*,([0-9]+)/\1/')
PARTS_SIZE=$(($PARTS_SIZE+1))
echo 'The part value is: '$PARTS_SIZE
echo ''


# Split item into needed parts 
echo 'Spliting the provided item into parts process had begun...'
split -b$PARTS_SIZE $FILE_PATH "${PART_PREFIX}" --numeric=1 --suffix-length=3

echo 'Process completed!'

# Retrive the number of parts
MAX_PART=$((($FILE_SIZE+$PARTS_SIZE-1)/$PARTS_SIZE))
echo 'The number of parts is: '$MAX_PART
echo ''

# Perform the PUT operation of parts
echo 'Perform the PUT operation of parts...'
for ((i=1; i<=$MAX_PART; i++))
do 
  PART_VALUE="${PART_PREFIX}"$(printf "%03d" $i)
  RESPONSE=$(curl -s -H 'Authorization: token '$ACCESS_TOKEN -X PUT "$UPLOAD_URL/$i" --data-binary @$PART_VALUE)
  echo "Done uploading part nr: $i/"$MAX_PART
done

echo 'Process was finished!'
echo ''

# Complete upload
echo 'Completing the file upload...'
RESPONSE=$(curl -s -H 'Authorization: token '$ACCESS_TOKEN -X POST "https://api.figshare.com/v2/account/articles/$ITEM_ID/files/$FILE_ID")
echo 'Done!'
echo ''

#remove the part files
rm "${PART_PREFIX}"*

# List all of the existing items
RESPONSE=$(curl -s -H 'Authorization: token '$ACCESS_TOKEN -X GET "https://api.figshare.com/v2/account/articles")
echo 'New list of items: '$RESPONSE
echo ''

