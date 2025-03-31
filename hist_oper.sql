do $$
    declare
        date_start date := '2024-12-03';
	    date_stop date := '2024-12-03';

        datePartBegin date:='2000-01-01'; --Эту дату не менять!!!

        maxStep integer := 3000; --Максимальное количество шагов
        rowsStep integer := 1000; --Количество строк за шаг

        id_start  bigint;
        id_stop  bigint;
        id_max  bigint;
        step  integer:=0;
        rowsAfect bigint;
        rowsAfectAll bigint:=0;

        part_start  bigint;
        part_stop  bigint;

    begin

        DROP TABLE IF EXISTS t;
        CREATE TEMPORARY TABLE t(
            id 			bigserial    not null PRIMARY KEY,
            OPERATION_DATE	DATE NOT NULL,
            EXT_ID		VARCHAR(32) NOT NULL,
            RECIPIENT_PHONE_NUMBER VARCHAR(15),
            STATUS_VALUE	VARCHAR(20),
            SENDER_INN	VARCHAR(12),
            SENDER_ACCOUNT VARCHAR(20),
            VERIFICATION_DATE	DATE,
            CUSTOM_PRODUCT_TYPE VARCHAR(12),
            RECIPIENT_EPK_ID VARCHAR(30),
            SENDER_BANK_ID	VARCHAR(12),
            SENDER_BIC VARCHAR(10)
        );

        part_start:=DATE_PART('Day', date_start::timestamp-datePartBegin::timestamp);
        part_stop:=DATE_PART('Day', date_stop::timestamp-datePartBegin::timestamp);

    for n in part_start..part_stop loop
        insert into t (RECIPIENT_PHONE_NUMBER, STATUS_VALUE,     SENDER_INN,     SENDER_ACCOUNT,        VERIFICATION_DATE,  CUSTOM_PRODUCT_TYPE,     RECIPIENT_EPK_ID,     SENDER_BANK_ID,     SENDER_BIC,     EXT_ID,     OPERATION_DATE)
        select trn.RECIPIENT_PHONE_NUMBER, trn.STATUS_VALUE, trn.SENDER_INN, trn.SENDER_ACCOUNT, trn.VERIFICATION_DATE, trn.CUSTOM_PRODUCT_TYPE, trn.RECIPIENT_EPK_ID, trn.SENDER_BANK_ID, trn.SENDER_BIC, trn.EXT_ID, trn.CREATE_DATE
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

        UPDATE sbp_b2c_history.history_operation ho
        SET RECIPIENT_PHONE_NUMBER = t.RECIPIENT_PHONE_NUMBER,
            STATUS_VALUE = t.STATUS_VALUE,
            SENDER_INN = t.SENDER_INN,
            SENDER_ACCOUNT = t.SENDER_ACCOUNT,
            VERIFICATION_DATE = t.VERIFICATION_DATE,
            CUSTOM_PRODUCT_TYPE = t.CUSTOM_PRODUCT_TYPE,
            RECIPIENT_EPK_ID = t.RECIPIENT_EPK_ID,
            SENDER_BANK_ID = t.SENDER_BANK_ID,
            SENDER_BIC = t.SENDER_BIC

        FROM t
        WHERE t.id>=id_start and t.id<=id_stop and t.EXT_ID = ho.EXT_ID and t.OPERATION_DATE=ho.OPERATION_DATE;

        GET DIAGNOSTICS rowsAfect = ROW_COUNT;
        COMMIT;

        raise notice 'step % id_start % id_stop % rowsAfect % time %',step, id_start,id_stop,rowsAfect,localtime;
        id_start:=id_stop;
        rowsAfectAll:=rowsAfectAll+rowsAfect;
    end loop;

    raise notice 'finish step: % id_stop: % id_max: % rowsAfectAll: % time: %',step, id_stop,id_max,rowsAfectAll,localtime ;

end$$;
