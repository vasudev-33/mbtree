DELIMITER $$

DROP PROCEDURE IF EXISTS `getRightCorrectnessKey` $$
CREATE DEFINER=`vasu`@`localhost` PROCEDURE `getRightCorrectnessKey`(IN levelId int, IN leafId int, IN key1 int, IN branchingFactor int, OUT correctnessLeafId int, OUT correctnessKey int)
BEGIN

  declare flag int;
  declare areSib int;
  declare leafId1 int;
  declare leafId2 int;
  declare curLevelId int;

  select  if(
          exists (select leaf_id, key_id from btree where level_id=levelId and leaf_id=leafId and key_id>key1),
          1,
          0
          ) into flag;
  if flag=1 then
      select leaf_id,min(key_id) into correctnessLeafId,correctnessKey from btree where level_id=levelId and leaf_id=leafId and key_id>key1;
  elseif flag=0 then
      select leaf_id,min(key_id) into correctnessLeafId,correctnessKey from btree where level_id=levelId and leaf_id=leafId+1;

      set leafId1=leafId;
      set leafId2=correctnessLeafId;
      set curLevelId=levelId;
      call areSiblings(leafId1, leafId2, branchingFactor, areSib);
      -- insert into tmplog values('areSib',areSib);
      -- insert into tmplog values('leafId1',leafId1);
      -- insert into tmplog values('leafId2',leafId2);
      while areSib=0 do
       set curLevelId=curLevelId-1;
       set leafId1=FLOOR(leafId1/branchingFactor);
       set leafId2=FLOOR(leafId2/branchingFactor);
       call areSiblings(leafId1, leafId2, branchingFactor, areSib);
       -- insert into tmplog values('areSib',areSib);
       -- insert into tmplog values('leafId1',leafId1);
       -- insert into tmplog values('leafId2',leafId2);
       if areSib=0 then
         call storeBoundaryLeafHashes(curLevelId,leafId2,branchingFactor,false);
       end if;
      end while;
  end if;

END $$

DELIMITER ;