size=1

while [[ "$#" > 1 ]]; do case $1 in
    --size) size="$2";;
    *) break;;
  esac; shift; shift
done

echo "Thread Pool Size = $size"

rm assign2/*.class
javac assign2/*.java
java assign2/MapReduce $size