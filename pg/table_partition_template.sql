DO $$DECLARE
    sql TEXT;
		tableTrigger TEXT;
		programArray INTEGER[];
		p INTEGER;
	  masterTable VARCHAR;
		masterTableName VARCHAR;
		partedColumn VARCHAR;
		pKey VARCHAR;
		i INTEGER;
BEGIN
	pkey = 'prog_id';
	masterTable := 'store.load_docs_log';
	masterTableName := 'load_docs_log';
	partedColumn := 'prog_id';

	sql := 'SELECT DISTINCT array_agg(id) from temp.p';

	EXECUTE sql into programArray;
	i := 0;

	--clean master table
	sql := 'select * INTO ' || masterTable || '_backup from ' || masterTable || ';';
	RAISE NOTICE 'sql: %', sql;

	sql := 'truncate table ' || masterTable || ';';
	RAISE NOTICE 'sql: %', sql;

	FOREACH p in ARRAY programArray
		LOOP
				sql := 'create table ' || masterTable|| '_' || p || ' ('
						    || ' constraint ' || masterTableName || '_' || p || '_pkey primary key (' || pKey || '),'
								|| ' constraint ' || masterTableName || '_' || p || '_check check (' || partedColumn || '= ' || p || ')'
							  || ')'
								|| ' inherits (' || masterTable || ');';

				RAISE NOTICE 'partition: %', sql;

				sql := 'create index ' || masterTableName || '_' || p || '_' || partedColumn || '_idx on ' || masterTable || '(' || partedColumn || ');';

				RAISE NOTICE 'index: %', sql;

				--create master table tigger for partitions
						IF i = 0 THEN
							tableTrigger := 'create or replace function ' || masterTable || '_insert_trigger() returns trigger language plpgsql as ' || '$' || '$' || ' BEGIN ';
							tableTrigger := tableTrigger || ' IF (NEW.' || partedColumn || '=' || p || ') THEN INSERT INTO ' || masterTable || '_' || p  || ' VALUES (NEW.*);';
						ELSE
							tableTrigger := tableTrigger || ' ELSIF (NEW.' || partedColumn || '=' || p || ') THEN INSERT INTO ' || masterTable || '_' || p || ' VALUES (NEW.*);';
						END IF;
						i := i+1;
		END LOOP;
				tableTrigger := tableTrigger || ' ELSE '
						|| ' RAISE EXCEPTION '''|| partedColumn || ' is out of range - fix partition_insert_trigger'';'
						|| ' END IF; return NULL; END;' || '$' || '$;';
				RAISE NOTICE 'trigger: %', tableTrigger;

	 			tableTrigger := 'create trigger ' || masterTableName || '_insert_trigger'
	 			|| ' before insert'
				|| ' on ' || masterTable
				|| ' for each row'
				|| ' execute procedure ' || masterTable || '_insert_trigger();';
				RAISE NOTICE 'trigger: %', tableTrigger;

				sql := 'insert into ' || masterTable || ' select * from ' || masterTable || '_backup;';
				RAISE NOTICE 'trigger: %', tableTrigger;
END$$;

