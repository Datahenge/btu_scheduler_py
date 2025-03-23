""" btu_py/cli/tests.py """

'''

	match matches.subcommand() {
		("test-pickler", Some(_)) => {
			cli_btu_test_pickler(&app_config, debug_mode);
		},
		("list-jobs", Some(_)) => {
			cli_list_jobs(&app_config);
		},
		("list-tasks", Some(_)) => {
			cli_list_tasks(&app_config);
		},
		("print-config", Some(_)) => {
			cli_print_config(&app_config);
		},
        ("queue-job-now", Some(arg_matches)) => {
            let job_id: &str = arg_matches.value_of("job_id").unwrap();
			cli_queue_job_immediately(&app_config, job_id);
		},
        ("queue-task-now", Some(arg_matches)) => {
            let task_id: &str = arg_matches.value_of("task_id").unwrap();
			cli_queue_task_immediately(&app_config, task_id);
		},
        ("show-scheduled", Some(_)) => {
			cli_show_scheduled_jobs(&app_config);
		},
		("show-job", Some(arg_matches)) => {
			let job_id: &str = arg_matches.value_of("job_id").unwrap();
			cli_show_job_details(&app_config, job_id);
		},
}



/*
    The remaining functions below are the "glue" between the CLI and the BTU library.
*/


fn cli_list_jobs(app_config: &AppConfig) {
    // Prints all jobs currently stored in Python RQ.
    match rq::get_all_job_ids(app_config) {
        Some(jobs) => {
            if jobs.len() == 0 {
                println!("No jobs were found in Python RQ.");
                return;
            }
            for job in jobs {
                println!("Job: '{}'", job);
            }
        },
        None => {
            println!("No jobs were found in Python RQ.");
        }
    }
}


/**
  Prints to console the ID and Description of all enabled BTU Tasks in the MariaDB database.
*/ 
fn cli_list_tasks(app_config: &AppConfig) {
    print_enabled_tasks(app_config, true);
}




fn cli_queue_job_immediately(app_config: &AppConfig, rq_job_id: &str) -> () {
    // Given an existing RQ Job, push it immediately into Redis Queue.
    if rq::exists_job_by_id(&app_config, &rq_job_id) {
        match rq::enqueue_job_immediate(&app_config, &rq_job_id) {
            Ok(ok_message) => {
                println!("{}", ok_message);
            }
            Err(err_message) => {
                println!("Error while attempting to queue job for execution: {}", err_message);
            }
        }
    }
    else {
        println!("Could not find a job with ID = {}", rq_job_id);
    }
}


fn cli_queue_task_immediately(app_config: &AppConfig, btu_task_id: &str) -> () {
    // 1. Create a Job, based on this Task.
    let task: BtuTask = BtuTask::new_from_mysql(btu_task_id, app_config);
    println!("Fetched task information from SQL: {}", task.task_key);
    println!("------\n{}\n------", task);

    // 2. Create an RQ Job from that Task.
    let rq_job: rq::RQJob = task.to_rq_job(app_config);
    println!("{}\n------", rq_job);

    // 3. Save the new Job into Redis.
    rq_job.save_to_redis(app_config);

    // 4. Enqueue that job for immediate execution.
    match rq::enqueue_job_immediate(&app_config, &rq_job.job_key_short) {
        Ok(ok_message) => {
            println!("Successfully enqueued: {}", ok_message);
        }
        Err(err_message) => {
            println!("Error while attempting to queue job for execution: {}", err_message);
        }
    }

    ()
}


fn cli_show_job_details(app_config: &AppConfig, job_id: &str) -> () {
	// println!("Attempting to fetch information about Job with ID = {}", job_id);
    match rq::read_job_by_id(app_config, job_id) {
        Ok(ok_message) => {
            println!("{}", ok_message);
        }
        Err(err_message) => {
            println!("{}", err_message);
        }
    }
}


fn cli_show_scheduled_jobs(app_config: &AppConfig) {
	scheduler::rq_print_scheduled_tasks(app_config, true);
}


'''
