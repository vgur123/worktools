--ALTER TABLE sbp_transfer ADD CONSTRAINT unique_suit UNIQUE (suit);

do $$
    declare
        date_start date := '2024-01-01';
	    date_stop date := '2024-01-30';

        maxStep     integer := 3000; --Max number of steps
        rowsStep    integer := 1000; --Rows per step

        id_start    bigint;
        id_stop     bigint;
        id_max      bigint;
        step        integer:=0;
        rowsAfect   bigint;
        rowsAfectAll bigint:=0;

		rec         RECORD;
	    n     		int8;

    begin
		--SET search_path = transfers_global_search_spb, "$user", public;

        DROP TABLE IF EXISTS tmp_table;
        CREATE TEMPORARY TABLE tmp_table(
               ID               bigint not null,
               OPERATION_DATE	DATE NOT NULL,
               EXT_ID		    VARCHAR(32) NOT NULL,
               PART             bigint,
               SUIT             VARCHAR(32),
               PRIMARY KEY (ID)
        );


        create index idx_suit on tmp_table (suit);

        
        insert into tmp_table (ID, OPERATION_DATE, EXT_ID, PART, SUIT)
        select row_number() OVER (order by OPERATION_DATE,EXT_ID) as ID,OPERATION_DATE, EXT_ID, CAST (to_char(OPERATION_DATE, 'YYYYMMDD') AS INTEGER), SUIT        from sbp_b2c_history.history_operation
        where OPERATION_DATE>=date_start and OPERATION_DATE<=date_stop
        order by OPERATION_DATE,EXT_ID;

        
        DELETE FROM tmp_table t1 USING sbp_transfer t2 WHERE  t2.suit = t1.suit;
        select Max(ID) into id_max from tmp_table;

        id_start=1;
        raise notice 'start maxStep: % rowsStep: % id_start: % id_max: % time: %',maxStep,rowsStep, id_start,id_max,localtime;

    --Основной цикл
    while step < maxStep and id_start <= id_max loop
        step := step + 1;
        id_stop:=id_start+rowsStep;
        if(id_stop>id_max) then id_stop:=id_max; end if;

        n := 0;
       
        for rec in select OPERATION_DATE, EXT_ID, PART
         from tmp_table
         where tmp_table.ID>=id_start and tmp_table.ID<id_stop
         loop
            insert into sbp_transfer (partition, suit, custom_product,      epk_id,                             transfer_version, create_date,    status,       transfer_sum, transfer_currency, card_num,         receiver_phone_number,  doc_id, nspk_id, receiver_first_name,  receiver_last_name,  receiver_middle_name)
            select                    rec.PART, SUIT,  upper(CUSTOM_PRODUCT_TYPE), CAST ((RECIPIENT_EPK_ID) AS BIGINT), 1,                OPERATION_DATE, STATUS_VALUE, PAYMENT_SUMMA, CURRENCY,         PAY_TOOL_NUMBER,  RECIPIENT_PHONE_NUMBER, DOC_ID, EXT_ID,  upper(RECIPIENT_FIRST_NAME), upper(RECIPIENT_LAST_NAME), upper(RECIPIENT_MIDDLE_NAME)
            from sbp_b2c_history.history_operation as hst
            where hst.OPERATION_DATE=rec.OPERATION_DATE and hst.EXT_ID=rec.EXT_ID;

            n:=n+1;
        end loop;

        COMMIT;

        raise notice 'step % id_start % id_stop % rowsAfect % time %',step, id_start,id_stop,n,localtime;
        id_start:=id_stop;
        rowsAfectAll:=rowsAfectAll+n;

    end loop;

    raise notice 'finish step: % id_stop: % id_max: % rowsAfectAll: % time: %',step, id_stop,id_max,rowsAfectAll,localtime ;

end$$;

