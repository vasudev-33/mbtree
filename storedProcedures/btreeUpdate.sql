DELIMITER $$

DROP PROCEDURE IF EXISTS `btreeUpdate` $$
CREATE DEFINER=`vasu`@`localhost` PROCEDURE `btreeUpdate`(In leftKey int, IN rightKey int, IN height int, IN newVal varchar(100), IN newHashes TEXT )
BEGIN


DECLARE pos INT;
DECLARE prevPos INT;
DECLARE len INT;

declare count1 int;

Declare levelId int;
declare leafId int;
declare hashval char(40);

set prevPos=0;
set pos = 0;
set len = 0;
set count1=0;
delete from testsplit;
SET autocommit=0;
WHILE LOCATE(',', newHashes, pos+1)>0 do

     set prevPos=pos;
     set pos = LOCATE(',', newHashes, pos+1); -- @pos
     set len = pos-prevPos-1;
     set levelId = CONVERT(SUBSTR(newHashes, prevPos+1, len), UNSIGNED INTEGER);

     set prevPos=pos;
     set pos = LOCATE(',', newHashes, pos+1); -- @pos
     set len = pos-prevPos-1;
     set leafId = CONVERT(SUBSTR(newHashes, prevPos+1, len), UNSIGNED INTEGER);

     set prevPos=pos;
     set pos = LOCATE(',', newHashes, pos+1); -- @pos
     set len = pos-prevPos-1;
     set hashVal = SUBSTR(newHashes, prevPos+1, len);
     -- insert into testsplit values(levelId,leafId,hashVal);
     update mbtree set hash_val=hashVal where level_id=levelId and leaf_id=leafId;

END while;


update btree set value1=CONCAT(key_id,':',newVal) where key_id>=leftKey and key_id<=rightKey and level_id=height;

-- COMMIT;

END $$

DELIMITER ;