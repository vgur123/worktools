-- clone table foreign_tables.transfer
-- CREATE TABLE sbp_b2c_history.transfer_backup AS TABLE foreign_tables.transfer WITH NO DATA;
do $$
    declare
        date_start timestamp := '2024-01-01 00:00:00'::timestamp;
        date_stop timestaclone tablemp := '2024-02-01 00:00:00'::timestamp;
        r sbp_b2c_history.transfer_backup%rowtype;
        n int8:=0;
    begin

       for r in SELECT * FROM foreign_tables.transfer
                WHERE nspk_send_date_time_info_rq >= date_start 
                and nspk_send_date_time_info_rq < date_stop loop

           INSERT INTO sbp_b2c_history.transfer_backup values (r.*);
           n:=n+1;
           if mod(n,1000)==0 then
               commit;
           end if;
    end loop;
    commit;
end$$;
