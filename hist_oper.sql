do $$
    declare
        date_start timestamp := '2024-01-01 00:00:00'::timestamp;
        date_stop timestamp := '2024-02-01 00:00:00'::timestamp;

        maxStep     integer := 3000; --Max steps
        rowsStep    integer := 1000; --Rows per step

        id_start    bigint;
        id_stop     bigint;
        id_max      bigint;
        step        integer:=0;
        rowsAfectAll bigint:=0;

		rec         RECORD;
		last_rec    RECORD;
	    n     		int8;

    begin

        DROP TABLE IF EXISTS t;
        CREATE TEMPORARY TABLE t(
            id 				    bigserial    not null PRIMARY KEY,
            OPERATION_DATE		DATE NOT NULL,
            EXT_ID			    VARCHAR(32) NOT NULL,
            RECIPIENT_LAST_NAME	    VARCHAR(100),
            RECIPIENT_FIRST_NAME	VARCHAR(100),
            RECIPIENT_MIDDLE_NAME	VARCHAR(100)
        );

        insert into t (RECIPIENT_LAST_NAME,    RECIPIENT_FIRST_NAME,     RECIPIENT_MIDDLE_NAME,     EXT_ID,             OPERATION_DATE)
        SELECT          last_name,              first_name,                 middle_name,            nspk_operation_id, nspk_send_date_time_info_rq
        FROM foreign_tables.transfer
        where nspk_send_date_time_info_rq >= date_start and nspk_send_date_time_info_rq <= date_stop
        and status in ('5','10')
        order by nspk_send_date_time_info_rq;

        select Max(id) into id_max from t;

        id_start=1;
        raise notice 'start maxStep: % rowsStep: % id_start: % id_max: % time: %',maxStep,rowsStep, id_start,id_max,localtime;

        --Main loop
        while step < maxStep and id_start <= id_max loop
         step := step + 1;
         id_stop:=id_start+rowsStep;
         if(id_stop>id_max) then id_stop:=id_max; end if;

		n := 0;
        for rec in select t.EXT_ID, t.OPERATION_DATE, t.RECIPIENT_LAST_NAME, t.RECIPIENT_FIRST_NAME, t.RECIPIENT_MIDDLE_NAME
           from t
           where t.id>=id_start and t.id<id_stop
         loop

            UPDATE sbp_b2c_history.history_operation ho
                SET 	RECIPIENT_LAST_NAME = rec.RECIPIENT_LAST_NAME,
                        RECIPIENT_FIRST_NAME = rec.RECIPIENT_FIRST_NAME,
                        RECIPIENT_MIDDLE_NAME = rec.RECIPIENT_MIDDLE_NAME
            WHERE ho.EXT_ID = rec.EXT_ID and ho.OPERATION_DATE = rec.OPERATION_DATE;

            n:=n+1;
        end loop;

        COMMIT;

        raise notice 'step % id_start % id_stop % rowsAfect % time %',step, id_start, id_stop, n, localtime;
        id_start:=id_stop;
        rowsAfectAll:=rowsAfectAll+n;
        if(id_start=id_max) then step = maxStep; end if;
    end loop;

    --Last record
    select t.EXT_ID, t.OPERATION_DATE, t.RECIPIENT_LAST_NAME, t.RECIPIENT_FIRST_NAME, t.RECIPIENT_MIDDLE_NAME  into last_rec
           from t
           where t.id=id_max;
    UPDATE sbp_b2c_history.history_operation ho
           SET 	RECIPIENT_LAST_NAME = last_rec.RECIPIENT_LAST_NAME,
                RECIPIENT_FIRST_NAME = last_rec.RECIPIENT_FIRST_NAME,
                RECIPIENT_MIDDLE_NAME = last_rec.RECIPIENT_MIDDLE_NAME
    WHERE ho.EXT_ID = last_rec.EXT_ID and ho.OPERATION_DATE = last_rec.OPERATION_DATE;
    rowsAfectAll:=rowsAfectAll+1;
    raise notice 'finish step: % id_stop: % id_max: % rowsAfectAll: % time: %',step, id_stop, id_max, rowsAfectAll, localtime ;

end$$;

