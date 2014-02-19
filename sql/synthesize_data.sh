#!/bin/bash

SOURCES_ROOT=sources
DIR_BASE=Home
DIR_TEMPLATE=${DIR_BASE}_0
NUM_SUBDIRS=10
NUM_BASE_FILES=50
NUM_SOURCES=20
NUM_SUBDIR_FILES=400






function genAnomalyPrevalence {

    ########## PREVALENCE ANOMALIES ################

    #now add the anomalie s
    NUM_PREVALENCE_ANOMALIES=5

    i=0

    while  [ $i -lt $NUM_PREVALENCE_ANOMALIES ]; do
        source_name=$((RANDOM % NUM_SOURCES))
        sub_dir=$((RANDOM % NUM_SUBDIRS))
        dd bs=100 count=10 if=/dev/urandom of=${SOURCES_ROOT}/${DIR_BASE}_${source_name}/${sub_dir}A_DIR/anom_${i} &>/dev/null
        i=$[$i+1]
    done

}


function genAnomalyLocality {

    echo "Generating anomalies for Locality Uniqueness"

    ############ LOCALITY UNIQUENESS ANOMALIES ########################

    #since loc unq is currently using time
    sleep 3

    NUM_LOCUNQ_ANOMALIES=5

    i=0

    while  [ $i -lt $NUM_LOCUNQ_ANOMALIES ]; do
        source_name=$((RANDOM % NUM_SOURCES))
        sub_dir=$((RANDOM % NUM_SUBDIRS))
        dd bs=100 count=10 if=/dev/urandom of=${SOURCES_ROOT}/${DIR_BASE}_${source_name}/${sub_dir}A_DIR/newer_anom_${i} &>/dev/null
        i=$[$i+1]
    done
}


function genAnomalyFileName {

    echo "Generating anomalies for file name"

    ############# FILE NAME ANOMALY ###############################

    i=0
    NUM_NAME_ANOMALIES=3

    while [ $i -lt $NUM_NAME_ANOMALIES ]; do

        #anomaly 1
        source_id=$((RANDOM % NUM_SOURCES))
        base_file=$((RANDOM % NUM_BASE_FILES))
        
        
        mv ${SOURCES_ROOT}/${DIR_BASE}_${source_id}/file_${base_file} ${SOURCES_ROOT}/${DIR_BASE}_${source_id}/diff_name_${base_file}
        i=$[$i+1]

    done

}





rm -rf $SOURCES_ROOT
mkdir $SOURCES_ROOT
mkdir ${SOURCES_ROOT}/${DIR_TEMPLATE}

i=0

#make subdir level A
while [ $i -lt ${NUM_SUBDIRS} ]; do
    mkdir "${SOURCES_ROOT}/${DIR_TEMPLATE}/${i}A_DIR"
    i=$[$i+1]
done


i=0

while [ $i -lt $NUM_BASE_FILES ]; do
    dd bs=32 count=10 if=/dev/urandom of="${SOURCES_ROOT}/${DIR_TEMPLATE}/file_${i}"  &>/dev/null
    i=$[$i+1]
done

i=0

while [ $i -lt $NUM_SUBDIR_FILES ]; do
    num=$((RANDOM % NUM_SUBDIRS))
    dd bs=50 count=10 if=/dev/urandom of="${SOURCES_ROOT}/${DIR_TEMPLATE}/${num}A_DIR/f_${i}"  &>/dev/null
    i=$[$i+1]
done


i=1

while [ $i -lt $(( NUM_SOURCES - 1 )) ]; do

    cp -rf ${SOURCES_ROOT}/${DIR_TEMPLATE} ${SOURCES_ROOT}/${DIR_BASE}_${i}
    i=$[$i+1]
done


#now create the anomalies
genAnomalyFileName
genAnomalyLocality
genAnomalyPrevalence

echo "Changing ownership of files to \"nobody\" which will require root"
sudo chown -R nobody:nobody ${SOURCES_ROOT}


#go ahead and create the csvs
#if [ -e filewalk.py ]; then
#    while [ $i -lt ${NUM_SOURCES} ]; do loc="${SOURCES_ROOT}/${DIR_TEMPLATE}_${i}"; python filewalk.py $loc test_os source_${i};  i=$[$i+1]; done
#fi
echo "done"


