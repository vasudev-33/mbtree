DELIMITER $$

DROP PROCEDURE IF EXISTS `storeBoundaryLeafHashes` $$
CREATE DEFINER=`vasu`@`localhost` PROCEDURE `storeBoundaryLeafHashes`(IN threadId int, in levelId int, in leafId int, in branchingFactor int, in isLeftBoundary int)
BEGIN
  declare boundaryLeafId int;
  declare modVal int;
  -- declare threadId int;

  -- set threadId=1;
  if isLeftBoundary=true then
    set boundaryLeafId=leafId;
    set modVal=boundaryLeafId MOD branchingFactor;
    back_loop : loop
      if modVal=0 then
        leave back_loop;
      else
        set boundaryLeafId=boundaryLeafId-1;
        set modVal=boundaryLeafId MOD branchingFactor;
      end if;
    end loop;


    SET @ins_bhash_tbl=CONCAT('insert into boundaryLeafHashTable',threadId,' (level_id,leaf_id,hash_val)
                              select level_id,leaf_id,hash_val
                              from mbtree where
                              leaf_id>=',boundaryLeafId,' and leaf_id<',leafId,' and level_id=',levelId );
    PREPARE stmt FROM @ins_bhash_tbl;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    -- insert into boundaryLeafHashTable (level_id,leaf_id,hash_val)
    -- select level_id,leaf_id,hash_val from mbtree where leaf_id>=boundaryLeafId and leaf_id<leafId and level_id=levelId;
  else
    set boundaryLeafId=leafId;
    set modVal=boundaryLeafId MOD branchingFactor;
    front_loop : loop
      if modVal=branchingFactor-1 then
        leave front_loop;
      else
        set boundaryLeafId=boundaryLeafId+1;
        set modVal=boundaryLeafId MOD branchingFactor;
      end if;
    end loop;

    SET @ins_bhash_tbl=CONCAT('insert into boundaryLeafHashTable',threadId,' (level_id,leaf_id,hash_val)
                              select level_id,leaf_id,hash_val
                              from mbtree where
                              leaf_id<=',boundaryLeafId,' and leaf_id>',leafId,' and level_id=',levelId );
    PREPARE stmt FROM @ins_bhash_tbl;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    -- insert into boundaryLeafHashTable (level_id,leaf_id,hash_val)
    -- select level_id,leaf_id,hash_val from mbtree where leaf_id<=boundaryLeafId and leaf_id>leafId and level_id=levelId;
  end if;

END $$

DELIMITER ;