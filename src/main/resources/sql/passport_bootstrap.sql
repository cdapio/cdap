SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

CREATE SCHEMA IF NOT EXISTS `continuuity` DEFAULT CHARACTER SET latin1 ;

USE `continuuity` ;

-- -----------------------------------------------------
-- Table `continuuity`.`account`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`account` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `name` VARCHAR(100) NOT NULL ,
  `email_id` VARCHAR(100) NOT NULL ,
  `salted_hashed_password` VARCHAR(100) NULL DEFAULT NULL ,
  `confirmed` TINYINT(1) NULL DEFAULT NULL ,
  `locked` TINYINT(1) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`account_payment`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`account_payment` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `account_id` INT(11) NOT NULL ,
  `credit_card_number` VARCHAR(20) NULL DEFAULT NULL ,
  `credit_card_name` VARCHAR(100) NULL DEFAULT NULL ,
  `credit_card_cvv` VARCHAR(6) NULL DEFAULT NULL ,
  `credit_card_expiration` VARCHAR(10) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  CONSTRAINT `account_payment_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `continuuity`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`account_role`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`account_role` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `account_id` INT(11) NOT NULL ,
  `name` VARCHAR(100) NULL DEFAULT NULL ,
  `permissions` VARCHAR(100) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  CONSTRAINT `account_role_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `continuuity`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`component_type`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`component_type` (
  `id` INT(11) NOT NULL ,
  `name` VARCHAR(100) NOT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`vpc_component`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`vpc_component` (
  `id` INT(11) NOT NULL ,
  `vpc_id` INT(11) NOT NULL ,
  `component_name` VARCHAR(100) NOT NULL ,
  `component_type` INT(11) NOT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `component_type` (`component_type` ASC) ,
  CONSTRAINT `vpc_component_ibfk_1`
    FOREIGN KEY (`component_type` )
    REFERENCES `continuuity`.`component_type` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`component_acls`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`component_acls` (
  `component_id` INT(11) NOT NULL ,
  `account_id` INT(11) NOT NULL ,
  `acl` VARCHAR(100) NULL DEFAULT NULL ,
  PRIMARY KEY (`component_id`, `account_id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  CONSTRAINT `component_acls_ibfk_1`
    FOREIGN KEY (`component_id` )
    REFERENCES `continuuity`.`vpc_component` (`id` ),
  CONSTRAINT `component_acls_ibfk_2`
    FOREIGN KEY (`account_id` )
    REFERENCES `continuuity`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`vpc`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`vpc` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `account_id` INT(11) NOT NULL ,
  `vpc_name` VARCHAR(100) NOT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  CONSTRAINT `vpc_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `continuuity`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`vpc_account`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`vpc_account` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `account_id` INT(11) NOT NULL ,
  `vpc_name` VARCHAR(100) NOT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  CONSTRAINT `vpc_account_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `continuuity`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `continuuity`.`vpc_roles`
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `continuuity`.`vpc_roles` (
  `vpc_id` INT(11) NOT NULL ,
  `account_id` INT(11) NOT NULL DEFAULT '0' ,
  `role_type` INT(11) NULL DEFAULT NULL ,
  `role_overrides` VARCHAR(100) NULL DEFAULT NULL ,
  PRIMARY KEY (`vpc_id`, `account_id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  INDEX `role_type` (`role_type` ASC) ,
  CONSTRAINT `vpc_roles_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `continuuity`.`account` (`id` ),
  CONSTRAINT `vpc_roles_ibfk_2`
    FOREIGN KEY (`role_type` )
    REFERENCES `continuuity`.`account_role` (`id` ),
  CONSTRAINT `vpc_roles_ibfk_3`
    FOREIGN KEY (`vpc_id` )
    REFERENCES `continuuity`.`vpc` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


