SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

CREATE SCHEMA IF NOT EXISTS `passport` DEFAULT CHARACTER SET latin1 ;
USE `passport` ;

-- -----------------------------------------------------
-- Table `passport`.`account`
-- Table to hold the account information
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`account` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `first_name` VARCHAR(50) NULL DEFAULT NULL ,
  `last_name` VARCHAR(50) NULL DEFAULT NULL ,
  `company` VARCHAR(100) NULL DEFAULT NULL,
  `email_id` VARCHAR(100) NOT NULL ,
  `salt`  VARCHAR(20) NULL DEFAULT NULL,
  `password` VARCHAR(100) NULL DEFAULT NULL ,
  `confirmed` TINYINT(1) NULL DEFAULT NULL ,
  `locked` TINYINT(1) NULL DEFAULT NULL ,
  `api_key` VARCHAR(100) NULL DEFAULT NULL,
  `account_created_at` DATETIME NOT NULL,
  `dev_suite_downloaded_at` DATETIME NULL DEFAULT NULL,
  `payment_account_id` VARCHAR(100) NULL DEFAULT NULL,
  `payment_info_provided_at` DATETIME NULL DEFAULT NULL,
  `org_id` VARCHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`id`) ,
  UNIQUE INDEX `email_id_UNIQUE` (`email_id` ASC),
  INDEX `org_id_idx` (`org_id` ASC),
  CONSTRAINT `org_id`
    FOREIGN KEY (`org_id` )
    REFERENCES `passport`.`organization` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `passport`.`account_payment`
-- Store payment information for the accounts
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`account_payment` (
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
    REFERENCES `passport`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `passport`.`account_role_types`
-- Stores role definitions for account. The role defnitions
-- can be used in any of the VPCs created in the account
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`account_role_types` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `account_id` INT(11) NOT NULL ,
  `NAME` VARCHAR(100) NULL DEFAULT NULL ,
  `permissions` VARCHAR(100) NULL DEFAULT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  CONSTRAINT `account_role_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `passport`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `passport`.`component_type`
-- Defines component type - Example: Datasets, Streams
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`component_type` (
  `id` INT(11) NOT NULL ,
  `NAME` VARCHAR(100) NOT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `passport`.`vpc_component`
-- Stores component information for each VPC
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`vpc_component` (
  `id` INT(11) NOT NULL ,
  `vpc_id` INT(11) NOT NULL ,
  `component_name` VARCHAR(100) NOT NULL ,
  `component_type` INT(11) NOT NULL ,
  PRIMARY KEY (`id`) ,
  INDEX `component_type` (`component_type` ASC) ,
  CONSTRAINT `vpc_component_ibfk_1`
    FOREIGN KEY (`component_type` )
    REFERENCES `passport`.`component_type` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `passport`.`component_acls`
-- Stores ACLS for each components
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`component_acls` (
  `component_id` INT(11) NOT NULL ,
  `account_id` INT(11) NOT NULL ,
  `acl` VARCHAR(100) NULL DEFAULT NULL ,
  PRIMARY KEY (`component_id`, `account_id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  CONSTRAINT `component_acls_ibfk_1`
    FOREIGN KEY (`component_id` )
    REFERENCES `passport`.`vpc_component` (`id` ),
  CONSTRAINT `component_acls_ibfk_2`
    FOREIGN KEY (`account_id` )
    REFERENCES `passport`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `passport`.`vpc_account`
-- Stores VPC for each account
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`vpc_account` (
  `id` INT(11) NOT NULL AUTO_INCREMENT ,
  `account_id` INT(11) NOT NULL ,
  `vpc_name` VARCHAR(100) NOT NULL ,
  `vpc_created_at` DATETIME NOT NULL,
  `vpc_label` VARCHAR(100),
  `vpc_type` VARCHAR(30) DEFAULT NULL,
  PRIMARY KEY (`id`) ,
  INDEX `account_id` (`account_id` ASC),
  UNIQUE INDEX `vpc_name_UNIQUE` (`vpc_name` ASC),
  CONSTRAINT `vpc_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `passport`.`account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;


-- -----------------------------------------------------
-- Table `passport`.`vpc_roles`
-- Store the role information for each user in the VPC
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`vpc_roles` (
  `vpc_id` INT(11) NOT NULL ,
  `account_id` INT(11) NOT NULL DEFAULT '0' ,
  `role_type` INT(11) NULL DEFAULT NULL ,
  `role_overrides` VARCHAR(100) NULL DEFAULT NULL ,
  PRIMARY KEY (`vpc_id`, `account_id`) ,
  INDEX `account_id` (`account_id` ASC) ,
  INDEX `role_type` (`role_type` ASC) ,
  CONSTRAINT `vpc_roles_ibfk_1`
    FOREIGN KEY (`account_id` )
    REFERENCES `passport`.`account` (`id` ),
  CONSTRAINT `vpc_roles_ibfk_2`
    FOREIGN KEY (`role_type` )
    REFERENCES `passport`.`account_role_types` (`id` ),
  CONSTRAINT `vpc_roles_ibfk_3`
    FOREIGN KEY (`vpc_id` )
    REFERENCES `passport`.`vpc_account` (`id` ))
ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

-- -----------------------------------------------------
-- Table `passport`.`nonce`
-- Entity to store nonce
-- -----------------------------------------------------

CREATE  TABLE IF NOT EXISTS `passport`.`nonce` (
  `nonce_id` INT(11) NOT NULL,
  `id` VARCHAR(100) NOT NULL ,
  `nonce_expires_at` DATETIME NOT NULL,
  UNIQUE INDEX `id_UNIQUE` (`id` ASC) ,
  UNIQUE INDEX `nonce_id_UNIQUE` (`nonce_id` ASC) )

-- -----------------------------------------------------
-- Table `passport`.`organization`
-- Entity to store organization info
-- -----------------------------------------------------
CREATE  TABLE IF NOT EXISTS `passport`.`organization` (
  `id` VARCHAR(100) NOT NULL,
  `name` VARCHAR(100) NOT NULL ,
  UNIQUE INDEX `id_UNIQUE` (`id` ASC)

ENGINE = InnoDB
DEFAULT CHARACTER SET = latin1;

USE `passport` ;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

