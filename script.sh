echo -e '\nrunning unit tests on all tables'
touch application_log.log
current_date_time=$(date)
echo "RUNNING THE APPLICATION AT: $current_date_time" >> application_log.log
echo -e '\nRUNNING INITIAL DATA QUALITY CHECK' >> application_log.log
python dev/initial_data_quality_test.py 2>&1 | tee -a application_log.log
echo -e '\nif all tests are passed, then please press lowercase y to continue with the program (any other key will cause the program to quit)'
read user_input
if [ "$user_input" = 'y' ]; then
    mkdir temporary_files
    echo -e '\ncleaning the students table'
    python dev/students_automated_cleaning.py
    echo -e '\nstudents table has been cleaned, press any key to continue.'
    read user_input
    echo -e '\ncleaning the jobs table'
    python dev/jobs_automated_cleaning.py
    echo -e '\njobs table has been cleaned, press any key to continue.'
    read user_input
    echo -e '\ncleaning the paths table'
    python dev/paths_automated_cleaning.py
    echo -e '\npaths table has been cleaned, press any key to run the final data quality check'
    read user_input
    echo -e '\nRUNNING FINAL DATA QUALITY CHECK' >> application_log.log
    python dev/final_data_quality_test.py 2>&1 | tee -a application_log.log
    echo -e '\nif all tests are passed, then please press lowercase y to continue with the program (any other key will cause the program to quit)'
    read user_input
    if [ "$user_input" = 'y' ]; then
       echo -e '\nloading data into the warehouse'
       python dev/loading_data_into_dwh.py
       echo -e '\ndisplaying the final warehouse'
       python prod/warehouse_viewer.py
    else
        exit
    fi
else
    exit
fi

rm -r temporary_files