do $$
    declare
        date_start date := '2024-12-03';
	    date_stop date := '2024-12-03';

        datePartBegin date:='2000-01-01'; --Do not change!!!

        maxStep integer := 3000; 	--Max steps
        rowsStep integer := 1000; 	--Records per step

        id_start  bigint;
        id_stop  bigint;
        id_max  bigint;
        step  integer:=0;
        rowsAfect bigint;
        rowsAfectAll bigint:=0;

        part_start  bigint;
        part_stop  bigint;
		
	odate 				date;
	extid 				VARCHAR(32);
	n     				int8;
        vRECIPIENT_LAST_NAME	    	VARCHAR(100);
        vRECIPIENT_FIRST_NAME		VARCHAR(100);
        vRECIPIENT_MIDDLE_NAME		VARCHAR(100);
    begin

        DROP TABLE IF EXISTS t;
        CREATE TEMPORARY TABLE t(
            id 				bigserial    not null PRIMARY KEY,
            OPERATION_DATE		DATE NOT NULL,
            EXT_ID			VARCHAR(32) NOT NULL,
            RECIPIENT_LAST_NAME	    	VARCHAR(100),
            RECIPIENT_FIRST_NAME	VARCHAR(100),
            RECIPIENT_MIDDLE_NAME	VARCHAR(100)
        );

        part_start:=DATE_PART('Day', date_start::timestamp-datePartBegin::timestamp);
        part_stop:=DATE_PART('Day', date_stop::timestamp-datePartBegin::timestamp);

    for n in part_start..part_stop loop
        insert into t (RECIPIENT_LAST_NAME,    RECIPIENT_FIRST_NAME,     RECIPIENT_MIDDLE_NAME,     EXT_ID,     OPERATION_DATE)
        select trn.RECIPIENT_LAST_NAME,    trn.RECIPIENT_FIRST_NAME, trn.RECIPIENT_MIDDLE_NAME, trn.EXT_ID, trn.CREATE_DATE
        from foreign_tables.incoming_transfer as trn
        where trn.part=n;
    end loop;

    select Max(id) into id_max from t;

    id_start=1;
    raise notice 'start maxStep: % rowsStep: % id_start: % id_max: % time: %',maxStep,rowsStep, id_start,id_max,localtime;

    while step < maxStep and id_start<id_max loop
         step := step + 1;
         id_stop:=id_start+rowsStep;
         if(id_stop>id_max) then id_stop:=id_max; end if;
          
		n := 0; 
		for       extid,    odate,          vRECIPIENT_LAST_NAME, vRECIPIENT_FIRST_NAME, vRECIPIENT_MIDDLE_NAME 
		 in select ext_id,  operation_date, t.RECIPIENT_LAST_NAME, t.RECIPIENT_FIRST_NAME, t.RECIPIENT_MIDDLE_NAME
		      from t 
			 where t.id>=id_start and t.id<=id_stop loop
			 
        	UPDATE sbp_b2c_history.history_operation ho
	        SET 	RECIPIENT_LAST_NAME = vRECIPIENT_LAST_NAME,
    	        	RECIPIENT_FIRST_NAME = vRECIPIENT_FIRST_NAME,
        	    	RECIPIENT_MIDDLE_NAME = vRECIPIENT_MIDDLE_NAME
        	WHERE extid = ho.EXT_ID and odate=ho.OPERATION_DATE;
			n:=n+1;
		end loop;
		
        COMMIT;

        raise notice 'step % id_start % id_stop % rowsAfect % time %',step, id_start,id_stop,n,localtime;
        id_start:=id_stop;
        rowsAfectAll:=rowsAfectAll+rowsAfect;
    end loop;

    raise notice 'finish step: % id_stop: % id_max: % rowsAfectAll: % time: %',step, id_stop,id_max,rowsAfectAll,localtime ;

end$$;
