alter table waves_blocks add column tx16calls int;
update waves_blocks set tx16calls = 0;
