DELIMITER $$

DROP PROCEDURE IF EXISTS `getLeftBoundaryKey` $$
CREATE DEFINER=`vasu`@`localhost` PROCEDURE `getLeftBoundaryKey`(in threadId int, in levelId int,in leafId int, in key1 int)
BEGIN

  declare done int DEFAULT false;
  declare curLevelId int;
  declare curLeafId int;
  declare curKeyId int;
  declare curBlob blob;
  declare curHashVal char(40);
  declare cur cursor for select level_id,leaf_id,key_id,value1 from btree where level_id=levelId and leaf_id=leafId order by key_id;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
  open cur;
  fetch cur into curLevelId,curLeafId,curKeyId,curBlob;
  while done=false do
    if curKeyId<key1 then
      set curHashVal=SHA1(curBlob);

      set @curLevelId=curLevelId;
      set @curLeafId=curLeafId;
      set @curKeyId=curKeyId;
      set @curHashVal=curHashVal;
      SET @ins_bkey_tbl=CONCAT('insert into boundaryKeyTable',threadId,' (level_id,leaf_id,key_id,hash_val) values (?,?,?,?)');
      PREPARE stmt FROM @ins_bkey_tbl;
      EXECUTE stmt using @curLevelId,@curLeafId,@curKeyId,@curHashVal ;
      DEALLOCATE PREPARE stmt;
      -- insert into boundaryKeyTable values(curLevelId,curLeafId,curKeyId,curHashVal);
    end if;
    fetch cur into curLevelId,curLeafId,curKeyId,curBlob;
  end while;
  close cur;
END $$

DELIMITER ;