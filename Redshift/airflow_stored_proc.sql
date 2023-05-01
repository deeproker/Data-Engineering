CREATE OR REPLACE PROCEDURE public.batch_ctl_logging(v_batch_name character varying(50), v_flag character varying(10))
 LANGUAGE plpgsql
AS $$
DECLARE
  status varchar(50);
  run_id int;
  batch_count int;
  batch_start timestamp;
BEGIN
      select into batch_count count(1) from  ctl_batch_run where batch_name =v_batch_name;
      IF batch_count = 0
        THEN 
        insert into ctl_batch_run 
        select v_batch_name,0,cast('01/01/1990' as timestamp),null,null,null ;
      END IF;
      select INTO batch_start batch_start_date from  airflow.public.ctl_batch_run where batch_name =v_batch_name;
      select INTO run_id batch_run_id+1  from  airflow.public.ctl_batch_run where batch_name =v_batch_name;
      select INTO status batch_status  from  airflow.public.ctl_batch_run where batch_name =v_batch_name;
      --select into run_id batch_run_id+1 from  ctl_batch_run where batch_name =v_batch_name;
      if status = 'COMPLETED' and v_flag = 'PRE'
      THEN
          UPDATE airflow.public.ctl_batch_run set last_extract_date=batch_start ,batch_run_id=run_id,
          batch_status='IN PROGRESS',BATCH_START_DATE =current_timestamp,BATCH_END_DATE=NULL 
          WHERE BATCH_NAME =v_batch_name;
      ELSIF status = 'IN PROGRESS' and v_flag = 'PRE'
      THEN
          UPDATE airflow.public.ctl_batch_run set BATCH_END_DATE=NULL 
          WHERE BATCH_NAME =v_batch_name;
      ELSIF status = 'IN PROGRESS' and v_flag = 'POST'
      THEN
            UPDATE airflow.public.ctl_batch_run set batch_status='COMPLETED',BATCH_END_DATE =current_timestamp 
          WHERE BATCH_NAME =v_batch_name;
      ELSIF status is NULL and v_flag='PRE'
      THEN 
        UPDATE airflow.public.ctl_batch_run set batch_run_id=run_id,
          batch_status='IN PROGRESS',BATCH_START_DATE =current_timestamp,BATCH_END_DATE=NULL 
          WHERE BATCH_NAME =v_batch_name;
      ELSE
        RAISE EXCEPTION 'Dag is already running cannot trigger other run';
      END IF;
END;
$$
