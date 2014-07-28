DELIMITER $$

DROP PROCEDURE IF EXISTS `areSiblings` $$
CREATE DEFINER=`vasu`@`localhost` PROCEDURE `areSiblings`(in leafId1 int,in leafId2 int, in branchingFactor int, out areSib int)
BEGIN
  declare parent1 int;
  declare parent2 int;

  set parent1=FLOOR(leafId1/branchingFactor);
  set parent2=FLOOR(leafId2/branchingFactor);
  -- insert into tmplog values('parent1',parent1);
  -- insert into tmplog values('parent2',parent2);
  if parent1<>parent2 then
    set areSib=0;
  else
    set areSib=1;
  end if;
END $$

DELIMITER ;