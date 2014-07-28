DELIMITER $$

DROP PROCEDURE IF EXISTS `searchKey` $$
CREATE DEFINER=`vasu`@`localhost` PROCEDURE `searchKey`(IN threadId int, IN key1 INT, IN branchingFactor INT, IN height int, IN isLeft int,OUT levelId int, OUT leafId int, OUT correctnessKey int)
BEGIN


  declare current_val int;
  declare leafIdCounter int;
  declare done int default FALSE;
  declare leftLeafId int;
  declare rightLeafId int;
  declare leftCorrectnessLeafId int;
  declare leftCorrectnessKey int;
  declare rightCorrectnessLeafId int;
  declare rightCorrectnessKey int;

  declare cur cursor for select key_id from btree where level_id=levelId and leaf_id=leafId order by key_id;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = true;
  set levelId=0;
  set leafId=0;

  query_loop: loop
    open cur;
    set height=height-1;
    if height<0 then
      leave query_loop;
    end if;
    set leafId=leafId*branchingFactor;
    set done=false;
    start_loop: loop
        fetch cur into current_val;
            if done then
                leave start_loop;
            elseif key1 < current_val then
                 leave start_loop;
            elseif key1 >= current_val then
                set leafId = leafId+1;
            end if;
    end loop;
    set levelId=levelId+1;
    close cur;

    if isLeft=true then
      if height=0 then
        call getLeftCorrectnessKey(levelId,leafId,key1,branchingFactor,leftCorrectnessLeafId,leftCorrectnessKey);
        call getLeftBoundaryKey(threadId, levelId,leftCorrectnessLeafId,leftCorrectnessKey);
        set  correctnessKey=leftCorrectnessKey;
        call storeBoundaryLeafHashes(threadId, levelId,leftCorrectnessLeafId,branchingFactor,true);
      else
        call storeBoundaryLeafHashes(threadId, levelId,leafId,branchingFactor,true);
      end if;
    else
      if height=0 then
        call getRightCorrectnessKey(levelId,leafId,key1,branchingFactor,rightCorrectnessLeafId,rightCorrectnessKey);
        call getRightBoundaryKey(threadId, levelId,rightCorrectnessLeafId,rightCorrectnessKey);
        set correctnessKey=rightCorrectnessKey;
        call storeBoundaryLeafHashes(threadId, levelId,rightCorrectnessLeafId,branchingFactor,false);
      else
        call storeBoundaryLeafHashes(threadId, levelId,leafId,branchingFactor,false);
      end if;
    end if;

  end loop;
END $$

DELIMITER ;