create sequence client_ids
start with 100
increment by 1;

drop sequence client_ids;

create sequence al_ids
start with 500
increment by 1;

drop sequence al_ids;

create table custom_TMP_clients (
       al_id number primary key,
       client_id number not null,
       name varchar2(32) not null,
       patronymic varchar2(64) null,
       surname varchar2(64) not null,
       al_cdate date default sysdate,
       t_changed varchar2(1 char) not null,
       al_udate date default sysdate,
       t_load_id number not null,
       begin_date timestamp not null,
       end_date timestamp default to_timestamp('12-Dec-5999 12:00:00', 'DD-Mon-RR HH24:MI:SS'),
       t_is_active number not null,
       constraint scd_unique unique (client_id, begin_date)       
)
partition by range (al_cdate)
interval(interval '1' month)
subpartition by hash (client_id)
subpartitions 8
(
       partition p1 values less than (date '2022-01-01')
)
compress;

create index  indx_custom_TMP_address_adr on 
custom_TMP_clients(client_id) local;

drop index indx_custom_TMP_address_adr;

create table i_TMP_clients (
       id number primary key,
       name varchar2(32) not null,
       patronymic varchar2(64) null,
       surname varchar2(64) not null,
       al_cdate date default sysdate,
       t_changed varchar2(1 char) not null
)
partition by range (al_cdate)
interval(interval '1' month)
subpartition by hash (id)
subpartitions 8
(
       partition p1 values less than (date '2022-01-01')
)
compress;

drop table custom_TMP_clients;
drop table i_TMP_clients;

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, end_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'John', 'Collins', '1', load_ids.nextval, to_timestamp('14-Jul-2022 15:39:00', 'DD-Mon-RR HH24:MI:SS'), to_timestamp('15-Dec-2022 12:00:00', 'DD-Mon-RR HH24:MI:SS'), 0);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.currval, 'John', 'Collins', '2', load_ids.nextval, sysdate - 1, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Lisa', 'Fox', '2', load_ids.nextval, sysdate - 1, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, end_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Kate', 'Patric', '1', load_ids.nextval, to_timestamp('17-Feb-2021 15:39:00', 'DD-Mon-RR HH24:MI:SS'), to_timestamp('13-Mar-2023 12:00:00', 'DD-Mon-RR HH24:MI:SS'), 0);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.currval, 'Kate', 'Patric', '2', load_ids.nextval, sysdate - 1, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Alex', 'Winston', '1', load_ids.nextval, sysdate - 1, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Molly', 'Chase', '2', load_ids.nextval, sysdate - 1, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Edward', 'Paker', '1', load_ids.nextval, sysdate - 1, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Tom', 'Shell', '1', load_ids.nextval, sysdate - 1, 1);

select * from custom_TMP_clients;
delete from custom_TMP_clients;

------------------------------------------

insert into i_TMP_clients(id, name, surname, t_changed) 
values (135, 'John', 'Collins', '2');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (136, 'Lisa', 'Fox', '2');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (137, 'Kate', 'Patric', '2');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (138, 'Alex', 'Malboro', '2');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (139, 'Molly', 'Fox', '2');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (140, 'Edward', 'Paker', '1');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (141, 'Tom', 'Shell', '1');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (client_ids.nextval, 'Jale', 'Spy', '1');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (client_ids.nextval, 'Cony', 'Newrby', '1');

insert into i_TMP_clients(id, name, surname, t_changed) 
values (client_ids.nextval, 'Vincent', 'Mario', '3');

select * from i_TMP_clients;

create or replace procedure scd2
       (load_id in number)
is
       count_dublicates number;
       dublicated exception;
begin
       with cd as (
       select client_id, count(*) over(partition by client_id) count_dubs
       from custom_TMP_clients
       where t_is_active = 1
       )
       
       select max(count_dubs) into count_dublicates from cd;
       
       if count_dublicates > 1
       then
         raise dublicated;
       end if;
             
       merge into custom_TMP_clients ctc
       using (select itcs.id, itcs.name, itcs.surname, itcs.patronymic
              from i_TMP_clients itcs
              left join custom_TMP_clients ctcs on ctcs.client_id = itcs.id
              where itcs.t_changed != 3 and ((ctcs.name != itcs.name or ctcs.surname != itcs.surname) and ctcs.client_id is not null or ctcs.client_id is null)
              ) uc 
       on (uc.id = ctc.client_id) 
       when matched then update set ctc.end_date = to_timestamp(to_char(trunc(current_timestamp - 1, 'DDD')) || ' 23:59:00', 'DD-Mon-RR HH24:MI:SS'),
                                    ctc.t_is_active = 2                -- промежуточное состояние для индикации только что изменённых строк 
       when not matched then insert (ctc.al_id,
                                     ctc.client_id,
                                     ctc.name,
                                     ctc.surname,
                                     ctc.patronymic,
                                     ctc.t_changed,
                                     ctc.al_cdate,
                                     ctc.t_load_id,
                                     ctc.begin_date,
                                     ctc.t_is_active)
                             values (al_ids.nextval,
                                     uc.id,
                                     uc.name,
                                     uc.surname,
                                     uc.patronymic,  
                                     '1',
                                     sysdate, 
                                     load_id,
                                     to_timestamp(to_char(trunc(current_timestamp, 'DDD')) || ' 00:00:00', 'DD-Mon-RR HH24:MI:SS'),
                                     1
                                     );
       
       insert into custom_TMP_clients (al_id,
                                     client_id,
                                     name,
                                     surname,
                                     patronymic,
                                     t_changed,
                                     al_cdate,
                                     t_load_id,
                                     begin_date,
                                     t_is_active)
       select al_ids.nextval,
              ctcs.client_id,
              itc.name,
              itc.surname,
              itc.patronymic,
              '2',
              sysdate,
              load_id,
              to_timestamp(to_char(trunc(current_timestamp, 'DDD')) || ' 00:00:00', 'DD-Mon-RR HH24:MI:SS'),
              1
       from custom_TMP_clients ctcs
       join i_TMP_clients itc on itc.id = ctcs.client_id
       where ctcs.t_is_active = 2;  
       
       update custom_TMP_clients set t_is_active = 0 where t_is_active = 2;     
exception
  when dublicated then
    dbms_output.put_line('there are dublicated active records in the target table');
end;

select * from i_TMP_clients order by id;
select * from custom_TMP_clients order by client_id, t_is_active;

begin
       scd2(550);
end;

select to_timestamp(to_char(trunc(current_timestamp - 1, 'DDD')) || ' 23:59:00', 'DD-Mon-RR HH24:MI:SS') from dual;
       
