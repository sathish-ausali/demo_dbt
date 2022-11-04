

  create or replace view `starlit-primacy-364713`.`DEMO_DATASET`.`my_second_dbt_model`
  OPTIONS()
  as -- Use the `ref` function to select from other models

select *
from `starlit-primacy-364713`.`DEMO_DATASET`.`my_first_dbt_model`
where id = 1;

