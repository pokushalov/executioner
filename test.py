from executioner import Executioner
from pprint import pprint
from loguru import logger
from pprint import pprint

# s = executioner.Executioner("./config/inventory.yaml")
s = Executioner("./config/inventory.yaml",hostname=None, group="oracle")
logger.info ("Test - initialized")
#xxx = s.runSQLwithResults('localhost', "XE", 'select file_name, tablespace_name, maxbytes  from dba_data_files;')
#print (xxx)
#s.run(hostname=None, group="test", command="date")

#s.run(hostname=None, group="test", command="ls -1 /home")
# to get runs
# xxx = s.run(command="ls -1 /")
# xxx = s.run(command="date")
# xxx = s.run(command="ps -ef | grep [p]mon | awk '{ print $(NF)}' | awk -F_ '{print $(NF)}'")
# xxx = s.run(command="ls -1 /")

# xxx = s.runSQLwithResults('localhost', "XE", "select table_name, owner from dba_tables where rownum < 10")

# exit(0)
# xxx = s.runSQLwithResults('localhost', "XE", "select 1 from dual;\r\n")
xxx = s.runSQLwithResults('manjaro_oracle', "XE", """
declare
    p_query        varchar2(32767) := 'select * from dba_data_files where rownum < 10';
    l_theCursor    integer default sys.dbms_sql.open_cursor;
    l_columnValue  varchar2(4000);
    l_status       integer;
    l_descTbl      sys.dbms_sql.desc_tab;
    l_colCnt       number;
    current_line   varchar2(4000);
    header_needed  boolean         := TRUE;
    current_header varchar2(4000);
    n              number          := 0;
    procedure p(msg varchar2) is
        l varchar2(4000) := msg;
    begin
        while length(l) > 0
            loop
                dbms_output.put_line(substr(l, 1, 80));
                l := substr(l, 81);
            end loop;
    end;
begin
    execute immediate
        'alter session set nls_date_format=''dd-mon-yyyy hh24:mi:ss'' ';

    sys.dbms_sql.parse(l_theCursor, p_query, sys.dbms_sql.native);
    sys.dbms_sql.describe_columns(l_theCursor, l_colCnt, l_descTbl);

    for i in 1 .. l_colCnt
        loop
            sys.dbms_sql.define_column(l_theCursor, i, l_columnValue, 4000);
        end loop;

    l_status := sys.dbms_sql.execute(l_theCursor);

    while (sys.dbms_sql.fetch_rows(l_theCursor) > 0)
        loop
            current_line := '';
            for i in 1 .. l_colCnt
                loop
                    sys.dbms_sql.column_value(l_theCursor, i, l_columnValue);
                    if length(current_line) > 0 then
                        current_line := current_line || ',';
                    end if;
                    current_line := current_line || '"' || l_columnValue || '"';
                    if header_needed then
                        if length(current_header) > 0 then
                            current_header := current_header || ',';
                        end if;
                        current_header := current_header || l_descTbl(i).col_name;
                    end if;

                    /*p( rpad( l_descTbl(i).col_name, 30 )
                      || ': ' ||
                      l_columnValue );

                    */
                end loop;
            if header_needed then
                DBMS_OUTPUT.put_line(current_header);
                header_needed := FALSE;
            end if;
            dbms_output.put_line(current_line);
            n := n + 1;
        end loop;
    if n = 0 then
        dbms_output.put_line('');
    end if;
end;
/
""")

print ("X" * 28)
pprint (xxx)
print ("Done")

s.close()

