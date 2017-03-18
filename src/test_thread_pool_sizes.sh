size=1
iterations=1 

while [[ "$#" > 1 ]]; do case $1 in
    --size) size="$2";;
    --iterations) iterations="$2";;
    *) break;;
  esac; shift; shift
done

echo "Thread Pool Size = $size"
echo "Number of Iterations = $iterations"

rm assign2/*.class
javac assign2/*.java
java assign2/TestMapReduceThreadPoolSizes $size $iterations