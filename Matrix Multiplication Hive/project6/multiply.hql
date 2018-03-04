drop table m_matrix_table;

drop table n_matrix_table;

create table m_matrix_table (
  m_ith_index bigint,
  m_jth_index bigint,
  m_value double
)
row format delimited
fields terminated by ','
stored as textfile;

create table n_matrix_table (
  n_jth_index bigint,
  n_kth_index bigint,
  n_value double
)
row format delimited
fields terminated by ','
stored as textfile;

load data local inpath '${hiveconf:M}' overwrite into table m_matrix_table;

load data local inpath '${hiveconf:N}' overwrite into table n_matrix_table;

select count(result_table.m_i_index) as count_records,
avg(result_table.product_value) as product_average
from 
(select m_matrix.m_ith_index as m_i_index,
n_matrix.n_kth_index as n_k_index,
sum(m_matrix.m_value * n_matrix.n_value) as product_value
from m_matrix_table m_matrix,
n_matrix_table n_matrix
where m_matrix.m_jth_index = n_matrix.n_jth_index
group by m_matrix.m_ith_index , n_matrix.n_kth_index) result_table;
