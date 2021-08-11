#!/bin/bash

# set the following env variable:
WORKDIR="$PWD"
RESULT_DIR="${WORKDIR}/results/"
FIO_JOBDIR="jobs.fio/"


echo "running fio on ${WORKDIR}"

mkdir -p ${RESULT_DIR}

for bs_size in 8 16 64 128 1024
do

        BS="${bs_size}k" fio ${FIO_JOBDIR}/randread.fio --output-format=json > ${RESULT_DIR}/randread.${bs_size}k.result.json

        BS="${bs_size}k" fio ${FIO_JOBDIR}/randwrite.fio --output-format=json > ${RESULT_DIR}/randwrite.${bs_size}k.result.json

        BS="${bs_size}k" fio ${FIO_JOBDIR}/read.fio --output-format=json > ${RESULT_DIR}/read.${bs_size}k.result.json

        BS="${bs_size}k" fio ${FIO_JOBDIR}/write.fio --output-format=json > ${RESULT_DIR}/write.${bs_size}k.result.json

done

rm ${WORKDIR}/fio.raw