sudo perf script > out.perf
git clone https://github.com/brendangregg/Flamegraph.git
perl Flamegraph/stackcollapse-perf.pl out.perf > out.folded
perl Flamegraph/flamegraph.pl out.folded > flamegraph.svg

mv flamegraph.svg /data/zjg/transferfile