/*
 Navicat Premium Data Transfer

 Source Server         : 101.236.37.48_3306
 Source Server Type    : MySQL
 Source Server Version : 50724
 Source Host           : 101.236.37.48:3306
 Source Schema         : TT2_DATA

 Target Server Type    : MySQL
 Target Server Version : 50724
 File Encoding         : 65001

 Date: 14/01/2019 18:22:08
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for article_lose
-- ----------------------------
DROP TABLE IF EXISTS `article_lose`;
CREATE TABLE `article_lose` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `url` text COMMENT '文章链接',
  `media_id` bigint(20) DEFAULT NULL COMMENT '用户Id',
  `article_id` bigint(20) DEFAULT NULL COMMENT '文章Id',
  `page_num` bigint(13) DEFAULT NULL COMMENT '页数',
  `keyword` varchar(100) NOT NULL COMMENT '关键字',
  `retry` tinyint(2) DEFAULT '0' COMMENT '重试次数',
  `type` tinyint(2) DEFAULT '0' COMMENT '是否爬取中',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `article_Id` (`article_id`) USING BTREE,
  KEY `media_id` (`media_id`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=43 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for articles
-- ----------------------------
DROP TABLE IF EXISTS `articles`;
CREATE TABLE `articles` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `url` text COMMENT '数据来源url',
  `crawl_time` datetime DEFAULT NULL COMMENT '抓取日期',
  `crawl_timestamp` int(11) DEFAULT NULL COMMENT '抓取的时间戳',
  `create_time` datetime DEFAULT NULL COMMENT '文章发布的时间',
  `create_timestamp` int(11) DEFAULT NULL COMMENT '文章发布的时间戳',
  `raw` mediumblob COMMENT '请求对应的原始html，compress存储',
  `status` int(11) DEFAULT '0' COMMENT '文章状态，0是爬取成功，1是解析\n内容成功',
  `media_id` varchar(64) NOT NULL COMMENT '自媒体的账号ID',
  `article_id` varchar(64) NOT NULL COMMENT '文章ID',
  `meta` mediumblob,
  `last_ts` int(10) DEFAULT '0',
  `retry` tinyint(2) DEFAULT '0' COMMENT '重试次数',
  `page_num` int(10) NOT NULL DEFAULT '0' COMMENT '页数',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `article_id` (`article_id`) USING BTREE,
  KEY `last_ts` (`last_ts`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=469 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for comments
-- ----------------------------
DROP TABLE IF EXISTS `comments`;
CREATE TABLE `comments` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `url` varchar(255) DEFAULT NULL COMMENT '评论的第一页的URL，如果 没有，为null',
  `crawl_time` datetime DEFAULT NULL COMMENT '抓取日期',
  `crawl_timestamp` int(11) DEFAULT NULL COMMENT '抓取时间戳',
  `create_time` datetime DEFAULT NULL COMMENT '评论的产生时间，此处\n为null',
  `create_timestamp` int(11) DEFAULT NULL COMMENT '评论的产生时间戳，\n此处为null',
  `page_num` int(10) NOT NULL COMMENT '评论页数',
  `raw` mediumblob COMMENT '评论内容压缩存储，compress存储',
  `media_id` varchar(64) NOT NULL COMMENT '被评论的文章的自媒体账号\nID',
  `article_id` varchar(64) NOT NULL COMMENT '被评论的文章ID',
  `meta` mediumblob,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `artId_pageNum` (`article_id`,`page_num`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for key_word
-- ----------------------------
DROP TABLE IF EXISTS `key_word`;
CREATE TABLE `key_word` (
  `id` int(10) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `keyword` varchar(10) CHARACTER SET utf8 DEFAULT NULL,
  `last_ts` int(10) DEFAULT '0',
  `last_ts_profile` int(10) DEFAULT '0',
  `retry` tinyint(2) DEFAULT '0',
  `retry_profile` tinyint(2) DEFAULT '0',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `keyword` (`keyword`) USING BTREE COMMENT '关键词索引',
  KEY `last_ts` (`last_ts`) USING BTREE COMMENT '时间索引',
  KEY `retry` (`retry`) USING BTREE,
  KEY `last_ts_profile` (`last_ts_profile`) USING BTREE,
  KEY `retry_profile` (`retry_profile`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=21129 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for user_profiles
-- ----------------------------
DROP TABLE IF EXISTS `user_profiles`;
CREATE TABLE `user_profiles` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `url` varchar(255) DEFAULT NULL COMMENT '账号profile主页',
  `crawl_time` datetime DEFAULT NULL COMMENT '抓取日期',
  `crawl_timestamp` int(11) DEFAULT NULL COMMENT '抓取时间戳',
  `create_time` datetime DEFAULT NULL COMMENT 'profile的注册时间， 没\n有为null',
  `create_timestamp` int(11) DEFAULT NULL COMMENT 'profile的注册时间\n戳， 没有为null',
  `raw` blob COMMENT 'profile的原始html, compress存储',
  `media_id` varchar(64) DEFAULT NULL,
  `status` int(11) DEFAULT '0' COMMENT '自媒体号的状态0是正常，非0不正\n常,如限制，封号，删号',
  `user_id` varchar(64) DEFAULT NULL COMMENT '自媒体账号ID',
  `name` varchar(64) DEFAULT NULL COMMENT '自媒体账号名称',
  `description` varchar(255) DEFAULT NULL COMMENT '自媒体账号描述',
  `follow_num` bigint(20) DEFAULT '0' COMMENT '关注数',
  `page_num` bigint(13) DEFAULT '0' COMMENT '页数',
  `meta` mediumblob,
  `last_ts` int(10) DEFAULT '0',
  `retry` tinyint(2) DEFAULT '0' COMMENT '重试次数',
  `type` tinyint(2) DEFAULT '0' COMMENT '优先账号',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `media_id` (`user_id`) USING BTREE,
  KEY `last_ts` (`last_ts`) USING BTREE,
  KEY `retry` (`retry`) USING BTREE
) ENGINE=MyISAM AUTO_INCREMENT=4980 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Procedure structure for get_keyword
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_keyword`;
delimiter ;;
CREATE DEFINER=`xxc`@`%` PROCEDURE `get_keyword`(IN `i_current_ts` int,
                               IN `i_set_ts` int,
                               OUT `o_url` varchar(255),
                               OUT `o_media_id` varchar(64),
                               OUT `o_name` varchar(64),
                               OUT `o_pageNum` int)
BEGIN
  SELECT url, media_id, name, page_num INTO o_url, o_media_id, o_name, o_pageNum FROM user_profiles WHERE last_ts < i_current_ts AND retry < 10 and type=0 and user_id != '' ORDER BY last_ts ASC LIMIT 1;
  UPDATE user_profiles SET last_ts = i_set_ts WHERE media_id = o_media_id;
END;
;;
delimiter ;

-- ----------------------------
-- Procedure structure for get_keyword_comment
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_keyword_comment`;
delimiter ;;
CREATE DEFINER=`xxc`@`%` PROCEDURE `get_keyword_comment`(IN `i_current_ts` int,
                               IN `i_set_ts` int,
                               OUT `o_article_id` varchar(64),
                               OUT `o_media_id` varchar(64),
                               OUT `o_url` varchar(255))
BEGIN
  select article_id,media_id,url into o_article_id,o_media_id,o_url FROM articles WHERE last_ts < i_current_ts AND retry < 10 ORDER BY last_ts ASC LIMIT 1;
  UPDATE articles SET last_ts = i_set_ts WHERE article_id = o_article_id;
END;
;;
delimiter ;

-- ----------------------------
-- Procedure structure for get_keyword_lost
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_keyword_lost`;
delimiter ;;
CREATE DEFINER=`xxc`@`%` PROCEDURE `get_keyword_lost`(OUT `o_url` varchar(255),
                               OUT `o_article_id` varchar(64),
                               OUT `o_media_id` varchar(64),
                               OUT `o_pageNum` int,
                               OUT `o_keyword` varchar(255))
BEGIN
  select url,article_id,media_id,page_num,keyword INTO o_url, o_article_id, o_media_id, o_pageNum, o_keyword from article_lose where retry<10 and type = 0 order by retry ASC limit 1;
  UPDATE article_lose SET type = 1 WHERE article_id = o_article_id and page_num = o_pageNum;
END;
;;
delimiter ;

-- ----------------------------
-- Procedure structure for get_keyword_prlority
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_keyword_prlority`;
delimiter ;;
CREATE DEFINER=`xxc`@`%` PROCEDURE `get_keyword_prlority`(IN `i_current_ts` int,
                               IN `i_set_ts` int,
                               OUT `o_url` varchar(255),
                               OUT `o_media_id` varchar(64),
                               OUT `o_name` varchar(64),
                               OUT `o_pageNum` int)
BEGIN
  SELECT url, media_id, name, page_num INTO o_url, o_media_id, o_name, o_pageNum FROM user_profiles WHERE last_ts < i_current_ts AND retry < 10 and type=1 ORDER BY last_ts ASC LIMIT 1;
  UPDATE user_profiles SET last_ts = i_set_ts WHERE media_id = o_media_id;
END;
;;
delimiter ;

-- ----------------------------
-- Procedure structure for get_keyword_profile
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_keyword_profile`;
delimiter ;;
CREATE DEFINER=`xxc`@`%` PROCEDURE `get_keyword_profile`(IN `i_current_ts` int,
                                       IN `i_set_ts` int,
                                       OUT `o_keyword` varchar(255),
                                       OUT `o_id` varchar(64))
BEGIN
  select keyword,id INTO o_keyword,o_id from key_word  WHERE last_ts_profile < i_current_ts AND retry < 10 ORDER BY last_ts ASC LIMIT 1;
  UPDATE key_word SET last_ts_profile = i_set_ts WHERE id = o_id;
END;
;;
delimiter ;

-- ----------------------------
-- Procedure structure for get_keyword_profile_article
-- ----------------------------
DROP PROCEDURE IF EXISTS `get_keyword_profile_article`;
delimiter ;;
CREATE DEFINER=`xxc`@`%` PROCEDURE `get_keyword_profile_article`(IN `i_current_ts` int,
                                       IN `i_set_ts` int,
                                       OUT `o_keyword` varchar(255),
                                       OUT `o_id` varchar(64))
BEGIN
  select keyword,id INTO o_keyword,o_id from key_word  WHERE last_ts_profile < i_current_ts AND retry < 10 ORDER BY last_ts ASC LIMIT 1;
  UPDATE key_word SET last_ts_profile = i_set_ts WHERE id = o_id;
END;
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
