drop table if exists test.randomDataSmall;
CREATE TABLE test.randomDataSmall(
       ID INT
     , CLUSTERED INT
     , SCATTERED INT
     , RANDOMISED INT
     , RANDOM_STRING VARCHAR(50)
     , SMALL_VC VARCHAR(50)
     , PADDING  VARCHAR(4000)
    )
STORED AS PARQUET;
desc test.randomDataSmall;
!exit


