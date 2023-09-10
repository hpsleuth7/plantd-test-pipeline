CREATE TABLE `supplier` (
  `supplier_id` varchar(255) NOT NULL,
  `supplier_name` varchar(255) NOT NULL,
  `produce_id` varchar(255), 
  `supplies_available` int NOT NULL,
  `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`supplier_id`),
  UNIQUE KEY `supplier_id_UNIQUE` (`supplier_id`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci