echo -e '\nrunning unit tests on all tables'
python initial_data_quality_test.py
echo -e '\nif all tests are passed, then please press lowercase y to continue with the program (any other key will cause the program to quit)'
read user_input
if [ "$user_input" = 'y' ]; then
    echo -e '\ncleaning the students table'
    python students_automated_cleaning.py
    echo -e '\ncleaning the jobs table'
    python jobs_automated_cleaning.py
    echo -e '\ncleaning the paths table'
    python paths_automated_cleaning.py
else
    exit
fi

