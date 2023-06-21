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
       al_cdate date default sysdate
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
values (al_ids.nextval, client_ids.currval, 'John', 'Collins', '2', load_ids.nextval, sysdate, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Lisa', 'Fox', '2', load_ids.nextval, sysdate, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, end_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Kate', 'Patric', '1', load_ids.nextval, to_timestamp('17-Feb-2021 15:39:00', 'DD-Mon-RR HH24:MI:SS'), to_timestamp('13-Mar-2023 12:00:00', 'DD-Mon-RR HH24:MI:SS'), 0);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.currval, 'Kate', 'Patric', '2', load_ids.nextval, sysdate, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Alex', 'Winston', '1', load_ids.nextval, sysdate, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Molly', 'Chase', '2', load_ids.nextval, sysdate, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Edward', 'Paker', '1', load_ids.nextval, sysdate, 1);

insert into custom_TMP_clients(al_id, client_id, name, surname, t_changed, t_load_id, begin_date, t_is_active) 
values (al_ids.nextval, client_ids.nextval, 'Tom', 'Shell', '1', load_ids.nextval, sysdate, 1);

select * from custom_TMP_clients;
delete from custom_TMP_clients;

------------------------------------------

insert into i_TMP_clients(id, name, surname) 
values (108, 'John', 'Collins');

insert into i_TMP_clients(id, name, surname) 
values (109, 'Lisa', 'Fox');

insert into i_TMP_clients(id, name, surname) 
values (110, 'Kate', 'Patric');

insert into i_TMP_clients(id, name, surname) 
values (111, 'Alex', 'Malboro');

insert into i_TMP_clients(id, name, surname) 
values (112, 'Molly', 'Fox');

insert into i_TMP_clients(id, name, surname) 
values (client_ids.nextval, 'Jale', 'Spy');

insert into i_TMP_clients(id, name, surname) 
values (client_ids.nextval, 'Cony', 'Newrby');

select * from i_TMP_clients;

create or replace procedure scd2
       (load_id in number)
is
       count_dublicates number;
       dublicated exception;
       
       cursor i_client_cur is
                 select * from i_TMP_clients order by id;
       i_client_rec i_client_cur%rowtype;
          
       cursor client_cur is
                 select * from custom_TMP_clients where t_is_active = 1 order by client_id;
       client_rec client_cur%rowtype;
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
       
       open i_client_cur;
       open client_cur;
       
       fetch i_client_cur into i_client_rec;     
       fetch client_cur into client_rec;
       
       loop
                   if i_client_rec.id = client_rec.client_id 
                   then
                       if i_client_rec.name != client_rec.name or i_client_rec.surname != client_rec.surname -- update
                       then
                          update custom_TMP_clients set name = i_client_rec.name, 
                                                        surname = i_client_rec.surname,
                                                        t_changed = '2',
                                                        t_load_id = load_id,
                                                        al_udate = sysdate,
                                                        t_is_active = 0,
                                                        end_date = to_timestamp(to_char(trunc(current_timestamp - 1, 'DDD')) || ' 23:59:00', 'DD-Mon-RR HH24:MI:SS')                                                     
                          where client_id = i_client_rec.id and t_is_active = 1;    
                       end if;
                       
                         fetch i_client_cur into i_client_rec;
                         exit when i_client_cur%notfound;
               
                         fetch client_cur into client_rec;
                         exit when client_cur%notfound;
                   else
                     insert into custom_TMP_clients(id, 
                                               name, 
                                               surname,
                                               al_cdate,
                                               t_changed,
                                               al_udate,
                                               t_load_id
                                               )
                     values (i_client_rec.id,
                             i_client_rec.name,
                             i_client_rec.surname,
                             i_client_rec.al_cdate,
                             '1',
                             sysdate,
                             load_id);
                     
                     fetch i_client_cur into i_client_rec;
                     exit when i_client_cur%notfound;
                   end if;                    
       end loop;
       
exception
  when dublicated then
    dbms_output.put_line('there are dublicated active records in target table');
end;

begin
       scd2(170);
end;

select to_timestamp(to_char(trunc(current_timestamp - 1, 'DDD')) || ' 23:59:00', 'DD-Mon-RR HH24:MI:SS') from dual;
       
