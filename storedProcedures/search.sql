DELIMITER $$

DROP PROCEDURE IF EXISTS `search` $$
CREATE DEFINER=`vasu`@`localhost` PROCEDURE `search`(IN threadId int, IN key1 INT, IN key2 INT, IN branchingFactor INT, IN height int)
BEGIN
  declare leftLevelId int default 0;
  declare leftLeafId int default 0;
  declare rightLevelId int default 0;
  declare rightLeafId int default 0;
  declare leftCompletenessKey int;
  declare rightCompletenessKey int;
  declare final_query varchar(1000);


  -- drop table if exists boundaryKeyTable;
  -- drop table if exists boundaryLeafHashTable;
  -- drop table if exists tmplog;
  -- create table if not exists boundaryLeafHashTable (level_id int, leaf_id int, hash_val char(40)) engine = memory;
  -- create table if not exists boundaryKeyTable (level_id int, leaf_id int, key_id int, hash_val char(40)) engine = memory;
  -- create table if not exists tmplog (msg varchar(512),thingId int) engine = memory;

   SET @del_bkey_tbl=CONCAT('delete from boundaryKeyTable',threadId);
   PREPARE stmt FROM @del_bkey_tbl;
   EXECUTE stmt;
  -- delete from boundaryKeyTable;
  -- delete from boundaryLeafHashTable;
   SET @del_bhash_tbl=CONCAT('delete from boundaryLeafHashTable',threadId);
   PREPARE stmt FROM @del_bhash_tbl;
   EXECUTE stmt;

  call searchKey(threadId, key1,branchingFactor,height,true,leftLevelId,leftLeafId,leftCompletenessKey);
  call searchKey(threadId, key2,branchingFactor,height,false,rightLevelId,rightLeafId,rightCompletenessKey);
  set @commaSelect=CONCAT('(select ","),');
  SET @countBoundaryKeyQuery=CONCAT('(select count(*) from boundarykeytable',threadId,'),');
  SET @countBoundaryHashQuery=CONCAT('(select count(*) from boundaryleafhashtable',threadId,'),');
  SET @minQuery=CONCAT('(select min(leaf_id) from btree where level_id=',rightLevelId,' and key_id>=',leftCompletenessKey,' and key_id<=',rightCompletenessKey,'),');
  SET @maxQuery=CONCAT('(select max(leaf_id) from btree where level_id=',rightLevelId,' and key_id>=',leftCompletenessKey,' and key_id<=',rightCompletenessKey,'))');
  SET @resultSet=CONCAT(' union (select CONCAT(key_id,',@commaSelect,' value1) from btree where level_id=',rightLevelId,' and key_id>=',leftCompletenessKey,' and key_id<=',rightCompletenessKey,')');
  SET @boundaryKeyTable=CONCAT(' union (select CONCAT(key_id,',@commaSelect,' hash_val) from boundarykeytable',threadId,')');
  SET @boundaryHashTable=CONCAT(' union (select CONCAT(level_id,',@commaSelect,'leaf_id,',@commaSelect,'hash_val) from boundaryleafhashtable',threadId,')');
  SET @final_result=CONCAT(
                    'select CONCAT(',
                    @countBoundaryKeyQuery,
                    @commaSelect,
                    @countBoundaryHashQuery,
                    @commaSelect,
                    @minQuery,
                    @commaSelect,
                    @maxQuery,
                    @resultSet,
                    @boundaryKeyTable,
                    @boundaryHashTable
                    );

  -- SET @final_result=CONCAT
  -- ('select CONCAT',
  -- '((select count(*) from boundarykeytable',threadId,')',',',
  -- '(select count(*) from boundaryleafhashtable',threadId,')',',',
  -- '(select CONCAT(min(leaf_id), max(leaf_id)) from btree where level_id=',rightLevelId,' and key_id>=',leftCompletenessKey,' and key_id<=',rightCompletenessKey,'))',
  -- ' union (select CONCAT(key_id, value1) from btree where level_id=',rightLevelId,' and key_id>=',leftCompletenessKey,' and key_id<=',rightCompletenessKey')',
  -- ' union (select CONCAT(key_id,hash_val) from boundarykeytable2)',
  -- ' union (select CONCAT(level_id,leaf_id,hash_val) from boundaryleafhashtable2)'
  -- );

  --
  -- ('select CONCAT',
  --
  --
  --
  --
  --
  --
  -- );

  SET final_query=@final_result;
  -- delete from tmplog;
  -- insert into tmplog values(final_query,1);

  PREPARE stmt FROM @final_result;
  EXECUTE stmt;
  -- select CONCAT
  -- ((select count(*) from boundarykeytable2),',',
  -- (select count(*) from boundaryleafhashtable2),',',
  -- (select CONCAT(min(leaf_id),',', max(leaf_id)) from btree where level_id=rightLevelId and key_id>=leftCompletenessKey and key_id<=rightCompletenessKey))

  -- union (select CONCAT(key_id,',', value1) from btree where level_id=rightLevelId and key_id>=leftCompletenessKey and key_id<=rightCompletenessKey)
  -- union (select CONCAT(key_id,',',hash_val) from boundarykeytable2)
  -- union (select CONCAT(level_id,',',leaf_id,',',hash_val) from boundaryleafhashtable2);

END $$

DELIMITER ;