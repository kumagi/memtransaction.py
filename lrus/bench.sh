#!/bin/zsh
target=(pylrubench lrubench repoze_lru)
for name in $target
do
    rm ${name}_out -f
    touch ${name}_out
done

python repoze_lru_bench.py 100000 100 >> repoze_lru_out

for ((size = 10000; size <= 100000; size+=10000));
do
    for ((num = 100000; num <= 1000000; num+= 100000));
    do
        for ((repeat = 0; repeat < 10; ++repeat));
        do
            for name in $target
            do
                echo "python ${name}bench.py ${num} ${size} >> ${name}_out"
            done
        done
    done
done
