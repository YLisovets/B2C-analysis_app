connect_to_db <- function() {

    con_db <- dbConnect(odbc(),
                     Driver = "SQL Server",
                     Server = Sys.getenv("SERVER_NAME"),
                     Database = Sys.getenv("DATABASE_NAME"),
                     UID = Sys.getenv("UID_DATABASE"),
                     PWD = Sys.getenv("PWD_DATABASE")
    )
    
}


connect_to_ukm <- function() {

    con_ukm <- dbConnect(MySQL(),
                     host = Sys.getenv("HOST_UKM"),
                     dbname = Sys.getenv("DBNAME_UKM"),
                     user = Sys.getenv("USER_UKM"),
                     password = Sys.getenv("PASSWORD_UKM")
    )
    
}


get_retail_discounts <- function(date_start, date_finish) {

    query <- str_c('SELECT t1.date, t1.client, t1.type, t1.global_number, t7.code_subdivision,
                        t2.name, t4.item, t4.total_quantity, t4.position,
                        t5.base_total, t5.increment, t5.account_increment
      FROM trm_out_receipt_header AS t1
      LEFT JOIN trm_out_receipt_discounts AS t2 ON
          t1.cash_id = t2.cash_id
      AND t1.id = t2.receipt_header
      LEFT JOIN trm_out_receipt_footer AS t3 ON
          t1.cash_id = t3.cash_id AND t3.id = t1.id
      LEFT JOIN trm_out_receipt_item AS t4 ON
          t4.receipt_header = t1.id AND t4.cash_id = t1.cash_id
      LEFT JOIN trm_out_receipt_item_discount AS t5 ON
          t4.cash_id = t5.cash_id AND t4.id = t5.receipt_item AND
          t2.id = t5.receipt_discount
      LEFT JOIN trm_in_pos AS t6 ON
          t1.cash_id = t6.cash_id
      LEFT JOIN trm_in_store AS t7 ON
          t6.store_id = t7.store_id
      WHERE t1.date between 
                CAST("',
      as.character(date_start),
      '" AS date) AND
                CAST("',
      as.character(date_finish - days(1)),
      '" AS date) AND t1.deleted = 0 AND t2.deleted = 0 AND t3.deleted = 0 AND
      t4.deleted = 0 AND t5.deleted = 0 AND t3.result = 0')
    
    tbl_query <- dbGetQuery(con_ukm, query) %>% 
        mutate(total_quantity = ifelse(type == 4,
                                       total_quantity * (-1),
                                       total_quantity),
               base_total = ifelse(type == 4,
                                   base_total * (-1),
                                   base_total),
               increment = ifelse(type == 4,
                                  increment * (-1),
                                  increment),
               account_increment = ifelse(type == 4,
                                          account_increment * (-1),
                                          account_increment))

    return(tbl_query)
}


get_new_cards <- function(date_start, date_finish) {

    query <- str_c('SELECT COUNT(*) AS count, store
                 FROM (
                    SELECT s.code_subdivision AS store, h.client, MIN(h.date) AS fdate
                    FROM trm_out_receipt_header AS h
                    LEFT JOIN trm_in_pos AS p ON h.cash_id = p.cash_id
                    LEFT JOIN trm_out_receipt_footer AS f ON h.cash_id = f.cash_id AND h.id = f.id
                    LEFT JOIN trm_in_store AS s ON p.store_id = s.store_id
                    WHERE h.deleted = 0 AND f.result = 0 AND f.deleted = 0 AND h.client IS NOT NULL
                    GROUP BY h.client) subquery
                 WHERE fdate BETWEEN CAST("',  as.character(date_start),'" AS date) AND
                                     CAST("',  as.character(date_finish - days(1)),'" AS date)
                 GROUP BY store')
    
    tbl_query <- dbGetQuery(con_ukm, query)

}


get_sale_service_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_sale_reg <- tbl(con_db,"_AccumRg17435") %>% 
        select(date = "_Period", active = "_Active",
               link_subdiv = "_Fld17441RRef",
               link_organization = "_Fld17443RRef",
               link_sale_doc = "_Fld17440_RRRef",
               link_cust_order_doc = "_Fld17438_RRRef",
               link_item = "_Fld17436RRef", link_project = "_Fld17442RRef",
               item_qty = "_Fld17445", item_sum = "_Fld17446",
               item_sum_without_disc = "_Fld17446",item_vat = "_Fld17448") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
    tbl_cost_reg <- tbl(con_db,"_AccumRg17487") %>% 
        select(date = "_Period", active = "_Active",
               link_subdiv = "_Fld17492RRef", link_project = "_Fld17493RRef",
               link_sale_doc = "_RecorderRRef",
               link_item = "_Fld17488RRef",
               item_qty = "_Fld17494", item_cost_sum = "_Fld17495",
               item_cost_vat = "_Fld17496") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")
    tbl_refund_doc <- tbl(con_db,"_Document251") %>% 
        select("_IDRRef", posted_refund_doc = "_Posted",
               is_managerial = "_Fld3903",
               nmbr_refund_doc = "_Number", date_refund_doc = "_Date_Time") %>% 
        mutate(posted_refund_doc = as.logical(posted_refund_doc),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date_refund_doc >= date_start,
               date_refund_doc < date_finish,
               posted_refund_doc == "TRUE",
               is_managerial == "TRUE")
    tbl_refund_doc_tbl <- tbl(con_db, "_Document251_VT3939") %>% 
        select(link_item = "_Fld3941RRef", item_qty = "_Fld3942",
               link_sale_doc = "_Fld3953_RRRef",
               "_Document251_IDRRef")
    tbl_checks <- tbl(con_db, "_Document329") %>% 
        select("_IDRRef", check_nmbr = "_Number",
               link_check_store = "_Fld7972RRef")
    tbl_sale_doc <- tbl(con_db,"_Document375") %>% 
        select("_IDRRef", sale_doc_nmbr = "_Number", sale_doc_date = "_Date_Time",
               link_customer = "_Fld10909RRef")
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description", subdiv_id = "_Code")    
    tbl_item <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", link_group = "_Fld2045RRef")
    tbl_item_group <- tbl(con_db, "_Reference122") %>% 
        select("_IDRRef", group_id = "_Code")
    tbl_project <- tbl(con_db, "_Reference143") %>% 
        select("_IDRRef", project_id = "_Code", project = "_Description")
    tbl_partner <- tbl(con_db, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    refund_doc <-  tbl_refund_doc %>% 
        left_join(tbl_refund_doc_tbl, by = c("_IDRRef" = "_Document251_IDRRef")) %>%
        left_join(tbl_item,           by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_sale_doc,       by = c(link_sale_doc = "_IDRRef")) %>%
        select(nmbr_refund_doc, date_refund_doc, item_id,
               refund_sale_doc_nmbr = sale_doc_nmbr,
               refund_sale_doc_date = sale_doc_date)
    
    cost_data <- tbl_cost_reg %>%
        left_join(tbl_item,       by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_checks,     by = c(link_sale_doc = "_IDRRef")) %>% 
        left_join(tbl_sale_doc,   by = c(link_sale_doc = "_IDRRef")) %>% 
        left_join(tbl_refund_doc, by = c(link_sale_doc = "_IDRRef")) %>%
        left_join(refund_doc,     by = c("nmbr_refund_doc", "date_refund_doc",
                                         "item_id")) %>%
        filter(!is.na(sale_doc_nmbr) | !is.na(refund_sale_doc_nmbr)) %>% 
        # select(sale_doc_nmbr, sale_doc_date, subdiv_id, project_id, item_id,
        #        item_qty, item_cost_sum,
        #        nmbr_refund_doc, date_refund_doc,
        #        refund_sale_doc_nmbr, refund_sale_doc_date
        #        ) %>% 
        mutate(sale_doc_nmbr = ifelse(!is.na(refund_sale_doc_nmbr),
                                      refund_sale_doc_nmbr,
                                      sale_doc_nmbr),
               sale_doc_date = ifelse(!is.na(refund_sale_doc_date),
                                      refund_sale_doc_date,
                                      sale_doc_date)) %>% 
        group_by(sale_doc_nmbr, sale_doc_date) %>%
        summarise(doc_cost_sum = sum(item_cost_sum) + sum(item_cost_vat)) %>%
        ungroup() %>%
        filter(doc_cost_sum > 0)
    
    distinct_sale_docs <- tbl_sale_reg %>% 
        filter(item_qty > 0) %>% 
        left_join(tbl_sale_doc,     by = c(link_sale_doc = "_IDRRef")) %>%
        filter(!is.na(sale_doc_nmbr)) %>%
        left_join(tbl_partner,      by = c(link_customer = "_IDRRef")) %>% 
        left_join(tbl_subdiv,       by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_project,      by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        select(sale_doc_nmbr, sale_doc_date, customer_id, subdiv_id,
               organization_id, project_id) %>% 
        distinct()

    df_sale <- tbl_sale_reg %>%
        left_join(tbl_checks,   by = c(link_sale_doc = "_IDRRef")) %>% 
        left_join(tbl_sale_doc, by = c(link_sale_doc = "_IDRRef")) %>% 
        left_join(tbl_item,     by = c(link_item = "_IDRRef")) %>% 
        group_by(sale_doc_nmbr, sale_doc_date) %>%
        summarise(doc_sale_sum = sum(item_sum) + sum(item_vat)) %>%
        ungroup() %>%
        filter(doc_sale_sum > 0) %>%
        left_join(cost_data,
                  by = c("sale_doc_nmbr", "sale_doc_date")) %>%
        left_join(distinct_sale_docs, 
                  by = c("sale_doc_nmbr", "sale_doc_date")) %>% 
        collect() %>% 
        replace_na(list(doc_cost_sum = 0)) %>% 
        mutate(gross_profit = doc_sale_sum - doc_cost_sum,
               sale_doc_date = as.Date(sale_doc_date) - years(2000))

    return(df_sale)
    
}

get_sale_retail_common <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_checks_tbl <- tbl(con_db, "_Document329_VT7992") %>% 
        select(check_nmbr = "_Fld8019", check_time = "_Fld8020",
               link_item = "_Fld7994RRef", item_qty = "_Fld7998",
               item_sum = "_Fld7999",
               "_Document329_IDRRef")
    tbl_checks_reqular <- tbl(con_db, "_Document329_VT8066") %>% 
        select(reg_cust_sum = "_Fld8070", "_Document329_IDRRef")
    tbl_checks_report <- tbl(con_db, "_Document329") %>% 
        select("_IDRRef", posted = "_Posted", is_managerial = "_Fld7964",
               date = "_Date_Time", nmbr_checks_report = "_Number",
               link_organization = "_Fld7962RRef",
               link_subdiv = "_Fld7966RRef") %>%
        mutate(posted = as.logical(posted),
               is_managerial = as.logical(is_managerial)) %>%
        filter(date >= date_start,
               date < date_finish,
               posted == "TRUE",
               is_managerial == "TRUE")
    
    tbl_cost_reg <- tbl(con_db,"_AccumRg17487") %>% 
        select(date_reg = "_Period", active = "_Active",
               link_sale_doc = "_RecorderRRef",
               link_item = "_Fld17488RRef",
               item_qty = "_Fld17494", item_cost_sum = "_Fld17495",
               item_cost_vat = "_Fld17496") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date_reg >= date_start,
               date_reg < date_finish,
               active == "TRUE")

    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    tbl_item <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_disc_card <- tbl(con_db, "_Reference86") %>% 
        select("_IDRRef", item_id = "_Code")

    cost_data <- tbl_cost_reg %>%
        left_join(tbl_item,          by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_checks_report, by = c(link_sale_doc = "_IDRRef")) %>% 
        filter(!is.na(nmbr_checks_report)) %>% 
        group_by(nmbr_checks_report, date) %>%
        summarise(doc_cost_sum = sum(item_cost_sum) + sum(item_cost_vat)) %>%
        ungroup() %>%
        filter(doc_cost_sum != 0)
    
    regular_cust_data <- tbl_checks_report %>% 
        left_join(tbl_checks_reqular,
                  by = c("_IDRRef" = "_Document329_IDRRef")) %>%
        group_by(nmbr_checks_report, date) %>%
        summarise(doc_reg_cust_sum = sum(reg_cust_sum),
                  doc_reg_cust_nmbr = n()) %>%
        ungroup() %>%
        filter(doc_reg_cust_sum != 0)

    
    df_checks <- tbl_checks_report %>% 
        left_join(tbl_checks_tbl,
                  by = c("_IDRRef" = "_Document329_IDRRef")) %>%
        left_join(tbl_item, by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_subdiv, by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        
        filter(!item_id %in% "000532240") %>%          # в Скарбничку
        
        group_by(date, nmbr_checks_report, subdiv_id, organization_id) %>%
        #mutate(concat = paste0(as.character(check_nmbr), "_", item_id)) %>% 
        summarise(checks_report_sum = sum(item_sum),
                  checks_report_qty = sum(item_qty),
                  total_checks_qty = n_distinct(check_nmbr),
                  checks_report_sku = n()
        ) %>%
        ungroup() %>%
        filter(checks_report_qty != 0) %>% 
        
        left_join(regular_cust_data,
                  by = c("date", "nmbr_checks_report")) %>%

        left_join(cost_data,
                  by = c("date", "nmbr_checks_report")) %>%
        
        collect()

    return(df_checks)

}


get_sale_retail_item_category <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_checks_tbl <- tbl(con_db, "_Document329_VT7992") %>% 
        select(check_nmbr = "_Fld8019", check_time = "_Fld8020",
               link_item = "_Fld7994RRef", item_qty = "_Fld7998",
               item_sum = "_Fld7999",
               "_Document329_IDRRef")
    tbl_checks_report <- tbl(con_db, "_Document329") %>% 
        select("_IDRRef", posted = "_Posted", is_managerial = "_Fld7964",
               date = "_Date_Time", nmbr_checks_report = "_Number",
               link_subdiv = "_Fld7966RRef") %>%
        mutate(posted = as.logical(posted),
               is_managerial = as.logical(is_managerial)) %>%
        filter(date >= date_start,
               date < date_finish,
               posted == "TRUE",
               is_managerial == "TRUE")

    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_item <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", link_group = "_Fld2045RRef")
    tbl_item_group <- tbl(con_db, "_Reference122") %>% 
        select("_IDRRef", group_id = "_Code", link_category = "_Fld19599RRef")
    tbl_category <- tbl(con_db, "_Reference19598") %>%
        select("_IDRRef", category_id = "_Code", category_name = "_Description")

    
    df_checks <- tbl_checks_report %>% 
        left_join(tbl_checks_tbl,
                  by = c("_IDRRef" = "_Document329_IDRRef")) %>%
        left_join(tbl_item, by = c(link_item = "_IDRRef")) %>%
        left_join(tbl_subdiv, by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_item_group, by = c(link_group = "_IDRRef")) %>%
        left_join(tbl_category,   by = c(link_category = "_IDRRef")) %>%
        filter(!is.na(category_id)) %>%
        
        mutate(subdiv_id = ifelse(subdiv_id %in% "000000189",   # Копіцентр ХМ СМ
                                  "000000007",
                                  subdiv_id)) %>% 

        group_by(subdiv_id, nmbr_checks_report, check_nmbr, category_id) %>%
        summarise(check_category_sum = sum(item_sum),
                  check_category_qty = sum(item_qty)
        ) %>%
        ungroup() %>%

        collect()

    return(df_checks)
    
}


get_sale_retail_category_cost <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    
    tbl_cost_reg <- tbl(con_db,"_AccumRg17487") %>% 
        select(date = "_Period", active = "_Active",
               link_subdiv = "_Fld17492RRef",
               link_sale_doc = "_RecorderRRef",
               link_item = "_Fld17488RRef",
               item_qty = "_Fld17494", item_cost_sum = "_Fld17495",
               item_cost_vat = "_Fld17496") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date >= date_start,
               date < date_finish,
               active == "TRUE")

    tbl_checks <- tbl(con_db, "_Document329") %>% 
        select("_IDRRef", check_nmbr = "_Number")
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description", subdiv_id = "_Code")    
    tbl_item <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", link_group = "_Fld2045RRef")
    tbl_item_group <- tbl(con_db, "_Reference122") %>% 
        select("_IDRRef", group_id = "_Code", link_category = "_Fld19599RRef")
    tbl_category <- tbl(con_db, "_Reference19598") %>%
        select("_IDRRef", category_id = "_Code", category_name = "_Description")


    cost_data <- tbl_cost_reg %>%
        left_join(tbl_item,       by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_checks,     by = c(link_sale_doc = "_IDRRef")) %>% 
        filter(!is.na(check_nmbr)) %>% 
        left_join(tbl_subdiv,     by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_item_group, by = c(link_group = "_IDRRef")) %>%
        left_join(tbl_category,   by = c(link_category = "_IDRRef")) %>%
        filter(!is.na(category_id)) %>% 
        
        mutate(subdiv_id = ifelse(subdiv_id %in% "000000189",   # Копіцентр ХМ СМ
                                  "000000007",                  # Хмельницький см
                                  subdiv_id)) %>% 
        
        # mutate(date = DATEADD(sql("month"),
        #                       DATEDIFF(sql("month"), 0, date),
        #                       0)) %>%

        group_by(subdiv_id, category_id) %>%
        summarise(category_cost_sum = sum(item_cost_sum) + sum(item_cost_vat)) %>%
        ungroup() %>% 
        collect()
    
    return(cost_data)

}


get_expenses <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_expenses <- tbl(con_db,"_AccumRg17131") %>% 
        select(date = "_Period", active = "_Active", type_exp = "_RecordKind",
               link_subdiv = "_Fld17132RRef", link_expItem = "_Fld17133RRef",
               link_project = "_Fld21330RRef",
               exp_sum = "_Fld17136", exp_vat = "_Fld17137") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE",
               date >= date_start,
               date < date_finish)
    
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_name = "_Description", subdiv_id = "_Code")
    tbl_project <- tbl(con_db,"_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_expItem <- tbl(con_db,"_Reference169") %>% 
        select("_IDRRef", expItems_id = "_Code", expItems_name = "_Description")
    
    df_expenses <- tbl_expenses %>%
        left_join(tbl_subdiv,  by = c(link_subdiv = "_IDRRef")) %>% 
        left_join(tbl_project, by = c(link_project = "_IDRRef")) %>% 
        left_join(tbl_expItem, by = c(link_expItem = "_IDRRef")) %>%
        filter(expItems_id != "100000421") %>% 
        select(date, expItems_id, expItems_name, subdiv_id, project,
               type_exp, exp_sum, exp_vat) %>%
        collect()
    
    return(df_expenses)
    
} 


get_compensation <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    
    tbl_charges <- tbl(con_db, "_Crg1030") %>% 
        select(date_charges = "_Period", is_active = "_Active",
               link_employee = "_Fld1032RRef",
               link_type_culc = "_CalcKindRRef",
               link_organization = "_Fld1031RRef",
               compensation_sum = "_Fld1036",
               link_subdiv = "_Fld1058RRef") %>% 
        mutate(is_active = as.logical(is_active)) %>% 
        filter(is_active == "TRUE",
               date_charges >= date_start,
               date_charges < date_finish,
               compensation_sum != 0)

    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    tbl_employees <- tbl(con_db, "_Reference159") %>% 
        select("_IDRRef", employee_name = "_Description", employee_id = "_Code",
               link_organization = "_Fld2324RRef",
               link_individ = "_Fld2322RRef") %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef"))
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_charges_type <- tbl(con_db,"_CKinds8") %>%
        select("_IDRRef", charges_type_id = "_Code",
               charges_type_name = "_Description")
    tbl_individuals <- tbl(con_db,"_Reference191") %>% 
        select("_IDRRef", individ_id ="_Code", individ_name = "_Description")
    tbl_workers <- tbl(con_db,"_InfoRg15330") %>%
        select(date_rec = "_Period", link_individ = "_Fld15331RRef",
               link_subdiv = "_Fld15332RRef", link_position = "_Fld15333RRef") %>% 
        filter(date_rec < date_finish)
    tbl_position <- tbl(con_db, "_Reference81") %>% 
        select("_IDRRef", position_name = "_Description", position_id = "_Code")

    data_workers <- tbl_workers %>% 
        left_join(tbl_individuals, by = c(link_individ = "_IDRRef")) %>% 
        left_join(tbl_position, by = c(link_position = "_IDRRef")) %>% 
        select(date_rec, individ_id, position_id) %>% 
        filter(!is.na(position_id)) %>% 
        group_by(individ_id) %>%
        filter(date_rec == max(date_rec, na.rm = TRUE)) %>% 
        ungroup() %>% 
        select(individ_id, position_id)
    
    
    data_compensation <- tbl_charges %>% 
        left_join(tbl_charges_type, by = c(link_type_culc = "_IDRRef")) %>%
        filter(charges_type_id == "00019") %>%
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_employees,    by = c(link_employee = "_IDRRef",
                                           "organization_id")) %>%
        left_join(tbl_subdiv,       by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_individuals,  by = c(link_individ = "_IDRRef")) %>%
        select(date = date_charges, individ_id, subdiv_id, compensation_sum) %>% 
        left_join(data_workers,     by = "individ_id")
    
    
    tax_rate <- tbl(con_db, "_Crg993") %>%
        select(date = "_Period", is_active = "_Active",
               link_employee = "_Fld995RRef",
               link_organization = "_Fld994RRef",
               link_type_tax = "_CalcKindRRef",
               tax_rate = "_Fld1007") %>% 
        mutate(is_active = as.logical(is_active)) %>% 
        filter(is_active == "TRUE",
               date >= date_start,
               date < date_finish,
               tax_rate > 0)
    
    tbl_taxes_type <- tbl(con_db,"_CKinds7") %>%
        select("_IDRRef", taxes_type_id = "_Code",
               taxes_type_name = "_Description")
    
    data_tax_rate <- tax_rate %>% 
        left_join(tbl_taxes_type,   by = c(link_type_tax = "_IDRRef")) %>%
        filter(taxes_type_id == "00014") %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        left_join(tbl_employees,    by = c(link_employee = "_IDRRef",
                                           "organization_id")) %>% 
        left_join(tbl_individuals,  by = c(link_individ = "_IDRRef")) %>%
        select(individ_id, tax_rate) %>% 
        distinct()

    
    df_compensation <- data_compensation %>%
        left_join(data_tax_rate, by = "individ_id") %>% 
        collect() %>% 
        mutate(total_salary_expenses = compensation_sum *
                   (1 + tax_rate + 0.195)) 

    return(df_compensation)

}


## Зарплата по работникам за период
salary_fn <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_payroll <- tbl(con_db,"_Document312") %>% 
        select(date = "_Date_Time", posted = "_Posted", period_reg = "_Fld6706",
               # link_subdiv_org = "_Fld6708RRef",
               is_managerial = "_Fld6712", "_IDRRef") %>% 
        mutate(posted = as.logical(posted),
               is_managerial = as.logical(is_managerial)) %>% 
        filter(date >= date_start,
               date < date_finish,
               posted == "TRUE",
               is_managerial == "TRUE")
    tbl_payroll_tbl_1 <- tbl(con_db,"_Document312_VT6743") %>% 
        select("_Document312_IDRRef", link_employee_org = "_Fld6745RRef",
               link_position = "_Fld18155RRef", link_subdiv = "_Fld6763RRef",
               salary = "_Fld6753",
               working_hours = "_Fld6756", norm_hours = "_Fld6758", )
    tbl_payroll_tbl_2 <- tbl(con_db,"_Document312_VT7084") %>% 
        select("_Document312_IDRRef", link_employee_org = "_Fld7086RRef",
               sum = "_Fld7096")
    tbl_payroll_tbl_3 <- tbl(con_db,"_Document312_VT7114") %>% 
        select("_Document312_IDRRef", link_employee_org = "_Fld7116RRef",
               sum = "_Fld7119")
    # tbl_subdiv_org <- tbl(con, "_Reference136") %>% 
    #     select("_IDRRef", subdiv_organiz_id = "_Code")    
    tbl_employee_org <- tbl(con_db, "_Reference159") %>% 
        select("_IDRRef", link_individ = "_Fld2322RRef")
    tbl_individuals <- tbl(con_db,"_Reference191") %>% 
        select("_IDRRef", individ_id = "_Code")
    tbl_position <- tbl(con_db,"_Reference81") %>% 
        select("_IDRRef", position_id = "_Code")
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code") 
    tbl_workers <- tbl(con_db,"_InfoRg15330") %>%
        select(date_rec = "_Period", link_individ = "_Fld15331RRef",
               link_subdiv = "_Fld15332RRef", link_position = "_Fld15333RRef") %>% 
        filter(date_rec < date_finish)

    data_workers <- tbl_workers %>% 
        left_join(tbl_individuals, by = c(link_individ = "_IDRRef")) %>% 
        left_join(tbl_position, by = c(link_position = "_IDRRef")) %>% 
        select(date_rec, individ_id, position_id) %>% 
        filter(!is.na(position_id)) %>% 
        group_by(individ_id) %>%
        filter(date_rec == max(date_rec, na.rm = TRUE)) %>% 
        ungroup() %>%
        select(individ_id, position_id)
    
    df_salary <- tbl_payroll %>%
        # left_join(tbl_subdiv_org,    by = c(link_subdiv_org = "_IDRRef")) %>%
        # filter(subdiv_organiz_id == subdiv_org) %>%
        left_join(tbl_payroll_tbl_1, by = c("_IDRRef"="_Document312_IDRRef")) %>% 
        left_join(tbl_subdiv,        by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_employee_org,  by = c(link_employee_org = "_IDRRef")) %>%
        left_join(tbl_individuals,   by = c(link_individ = "_IDRRef")) %>%
        left_join(data_workers,      by = "individ_id") %>%
        select(individ_id, subdiv_id, position_id, salary,
               working_hours, norm_hours) %>% 
        group_by(individ_id, subdiv_id, position_id) %>%
        summarise(salary_sum = sum(salary, na.rm = TRUE),
                  working_hours = sum(working_hours, na.rm = TRUE),
                  norm_hours = sum(norm_hours, na.rm = TRUE)) %>%
        ungroup() %>%
        collect() %>% 
        group_by(individ_id) %>% 
        mutate(subdiv_id = subdiv_id[!is.na(subdiv_id)][1L]) %>% 
        mutate(position_id = position_id[!is.na(position_id)][1L]) %>% 
        ungroup() %>% 
        filter(salary_sum != 0) %>% 
        group_by(individ_id, subdiv_id, position_id) %>% 
        summarise(salary_sum = sum(salary_sum),
                  working_hours = sum(working_hours),
                  norm_hours = sum(norm_hours)) %>% 
        ungroup() %>% 
        mutate(stuff_unit = case_when(
            norm_hours == 0              ~ 0,
            working_hours >= norm_hours  ~ 1,
            TRUE                         ~ working_hours / norm_hours)) %>% 
        select(-norm_hours)
    
    df_contribution <- tbl_payroll %>%
        # left_join(tbl_subdiv_org,    by = c(link_subdiv_org = "_IDRRef")) %>%
        # filter(subdiv_organiz_id == subdiv_org) %>%
        left_join(tbl_payroll_tbl_2, by = c("_IDRRef"="_Document312_IDRRef")) %>% 
        left_join(tbl_employee_org,  by = c(link_employee_org = "_IDRRef")) %>%
        left_join(tbl_individuals,   by = c(link_individ = "_IDRRef")) %>%
        select(individ_id, sum) %>% 
        group_by(individ_id) %>% 
        summarise(contr_sum = sum(sum, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect()
    
    df_taxes <- tbl_payroll %>%
        left_join(tbl_payroll_tbl_3, by = c("_IDRRef"="_Document312_IDRRef")) %>% 
        # left_join(tbl_subdiv_org,    by = c(link_subdiv_org = "_IDRRef")) %>%
        # filter(subdiv_organiz_id == subdiv_org) %>% 
        left_join(tbl_employee_org,  by = c(link_employee_org = "_IDRRef")) %>%
        left_join(tbl_individuals,   by = c(link_individ = "_IDRRef")) %>%
        select(individ_id, sum) %>% 
        group_by(individ_id) %>% 
        summarise(taxes_sum = sum(sum, na.rm = TRUE)) %>% 
        ungroup() %>% 
        collect()
    
    df_data <- df_salary %>% 
        left_join(df_contribution) %>% 
        left_join(df_taxes) %>% 
        mutate(across(where(is.numeric), ~ifelse(is.na(.), 0, .)),
               total_salary = salary_sum + contr_sum + taxes_sum) %>%
        mutate(subdiv_id = ifelse(subdiv_id %in% "000000189",   # Копіцентр ХМ СМ
                                  "000000007",
                                  subdiv_id)) %>%
        select(individ_id, subdiv_id, position_id, total_salary, stuff_unit,
               working_hours)

    return(df_data)
    
}


## Развозка
get_delivery_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    date_start_2 <- date_start - weeks(2)
    
    tbl_delivery <- tbl(con_db,"_Document416") %>% 
        select("_IDRRef", posted_delivery = "_Posted", nmbr_delivery = "_Number",
               date_delivery = "_Date_Time", 
               link_store = "_Fld12841RRef", 
               time_depart = "_Fld12837", time_return = "_Fld12838",
               link_direction = "_Fld12831RRef", link_route = "_Fld19689RRef",
               link_vehicle = "_Fld12834RRef", delivery_weight = "_Fld12830",
               speedo_start = "_Fld12835", speedo_finish = "_Fld12836") %>%
        mutate(posted_delivery = as.logical(posted_delivery)) %>% 
        filter(time_depart >= date_start,
               time_depart < date_finish,
               posted_delivery == "TRUE")
    tbl_delivery_tbl_forwarder <- tbl(con_db, "_Document416_VT12850") %>% 
        select(link_forwarder = "_Fld12852RRef", "_Document416_IDRRef")
    tbl_delivery_tbl_driver <- tbl(con_db, "_Document416_VT12853") %>% 
        select(link_driver = "_Fld12855RRef", "_Document416_IDRRef")
    tbl_delivery_tbl_fuel <- tbl(con_db, "_Document416_VT12867") %>% 
        select(link_fuel = "_Fld12869RRef", fuel_consumption = "_Fld21170",
               "_Document416_IDRRef")
    tbl_delivery_tbl_order_deliv <- tbl(con_db, "_Document416_VT12842") %>% 
        select(link_order_deliv = "_Fld12844_RRRef", "_Document416_IDRRef",
               to_intermediate = "_Fld12845", is_not_delivered = "_Fld12846") %>% 
        mutate(to_intermediate = as.logical(to_intermediate),
               is_not_delivered = as.logical(is_not_delivered))
    tbl_individuals <- tbl(con_db,"_Reference191") %>% 
        select("_IDRRef", user_id = "_Code")
    tbl_store <- tbl(con_db, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    tbl_direction <- tbl(con_db, "_Reference193") %>% 
        select("_IDRRef", direction = "_Description")
    tbl_route <- tbl(con_db, "_Reference19642") %>% 
        select("_IDRRef", route_id = "_Code", route = "_Description")
    tbl_vehicle <- tbl(con_db, "_Reference195") %>% 
        select("_IDRRef", vehicle_id = "_Code", vehicle_name = "_Description")
    tbl_deliv_order <- tbl(con_db,"_Document409") %>% 
        select("_IDRRef", posted_deliv_order = "_Posted", nmbr_deliv_order = "_Number",
               date_deliv_order = "_Date_Time") %>% 
        mutate(posted_deliv_order = as.logical(posted_deliv_order)) %>% 
        filter(date_deliv_order >= date_start_2,
               date_deliv_order < date_finish,
               posted_deliv_order == "TRUE")
    tbl_laggage <- tbl(con_db,"_Document417") %>% 
        select("_IDRRef", nmbr_laggage = "_Number", date_laggage = "_Date_Time")
    
    df_delivery <- tbl_delivery %>%
        left_join(tbl_delivery_tbl_forwarder, 
                  by = c("_IDRRef" = "_Document416_IDRRef")) %>% 
        left_join(select(tbl_individuals, "_IDRRef", forwarder = user_id),
                  by = c(link_forwarder = "_IDRRef")) %>%
        left_join(tbl_delivery_tbl_driver, 
                  by = c("_IDRRef" = "_Document416_IDRRef")) %>% 
        left_join(select(tbl_individuals, "_IDRRef", driver = user_id),
                  by = c(link_driver = "_IDRRef")) %>%
        left_join(tbl_store,       by = c(link_store = "_IDRRef")) %>%
        left_join(tbl_direction,   by = c(link_direction = "_IDRRef")) %>%
        left_join(tbl_route,       by = c(link_route = "_IDRRef")) %>%
        left_join(tbl_vehicle,     by = c(link_vehicle = "_IDRRef")) %>%
        left_join(tbl_delivery_tbl_order_deliv, 
                  by = c("_IDRRef" = "_Document416_IDRRef")) %>%
        left_join(tbl_deliv_order, by = c(link_order_deliv = "_IDRRef")) %>%
        left_join(tbl_laggage,     by = c(link_order_deliv = "_IDRRef")) %>%
        select(nmbr_delivery, date_delivery,
               forwarder, driver, vehicle_id, vehicle_name,
               direction, route_id, route, store_id,store_name, delivery_weight,
               time_depart, time_return, speedo_start, speedo_finish,
               # item_id, fuel_consumption,
               to_intermediate, is_not_delivered,
               nmbr_deliv_order, date_deliv_order,
               nmbr_laggage, date_laggage
        ) %>% 
        collect()
    
    return(df_delivery)

}


get_delivery_order_data <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    date_start_2 <- date_start - days(30)
    
    tbl_deliv_order <- tbl(con_db,"_Document409") %>% 
        select("_IDRRef", posted_deliv_order = "_Posted", nmbr_deliv_order = "_Number",
               date_deliv_order = "_Date_Time", weight = "_Fld12605",
               link_store = "_Fld12588_RRRef",
               link_address = "_Fld12593RRef", link_recipient = "_Fld12590_RRRef",
               link_intermediate = "_Fld12589RRef", nmbr_declar = "_Fld21329",
               link_document_base = "_Fld12587_RRRef") %>%
        mutate(posted_deliv_order = as.logical(posted_deliv_order)) %>% 
        filter(date_deliv_order >= date_start,
               date_deliv_order < date_finish,
               posted_deliv_order == "TRUE")

    tbl_store <- tbl(con_db, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    tbl_partner <- tbl(con_db, "_Reference102") %>% 
        select("_IDRRef", customer_id = "_Code")
    tbl_address <- tbl(con_db, "_Reference194") %>% 
        select("_IDRRef", deliv_address_id = "_Code",
               link_address_region = "_Fld2738RRef")
    tbl_region <- tbl(con_db, "_Reference149") %>% 
        select("_IDRRef", address_region = "_Code")
    tbl_sale_doc <- tbl(con_db,"_Document375") %>% 
        select("_IDRRef", nmbr_sale_doc = "_Number",
               date_sale_doc = "_Date_Time",
               link_sale_subdiv = "_Fld10904RRef") %>% 
        filter(date_sale_doc >= date_start_2,
               date_sale_doc < date_finish)
    tbl_moving_out <- tbl(con_db,"_Document411") %>% 
        select("_IDRRef", nmbr_moving_out = "_Number",
               date_moving_out = "_Date_Time") %>% 
        filter(date_moving_out >= date_start_2,
               date_moving_out < date_finish)
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")

    df_deliv_order <- tbl_deliv_order %>%
        left_join(tbl_store,       by = c(link_store = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", recipient_store = store_id),
                  by = c(link_recipient = "_IDRRef")) %>%
        left_join(tbl_partner,     by = c(link_recipient = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", intermediate_store = store_name),
                  by = c(link_intermediate = "_IDRRef")) %>%
        left_join(tbl_address,     by = c(link_address = "_IDRRef")) %>%
        left_join(tbl_region,      by = c(link_address_region = "_IDRRef")) %>%
        left_join(tbl_sale_doc,    by = c(link_document_base = "_IDRRef")) %>%
        left_join(tbl_subdiv,      by = c(link_sale_subdiv = "_IDRRef")) %>%
        left_join(tbl_moving_out,  by = c(link_document_base = "_IDRRef")) %>%
        select(nmbr_deliv_order, date_deliv_order,
               deliv_address_id, address_region,
               store_id, store_name, intermediate_store, weight,
               nmbr_sale_doc, date_sale_doc, customer_id, subdiv_id,
               nmbr_moving_out, date_moving_out, recipient_store
        ) %>% 
        collect()
    
    return(df_deliv_order)

}


# Графіки перевезення
get_delivery_schedule <- function() {

    tbl_delivery_schedule <- tbl(con,"_InfoRg18903") %>% 
        select(date = "_Period", 
               link_recipient_store = "_Fld18946RRef",
               link_distribution_store = "_Fld18944RRef",
               day_1 = "_Fld19445",
               day_2 = "_Fld19446",
               day_3 = "_Fld19447",
               day_4 = "_Fld19448",
               day_5 = "_Fld19449",
               day_6 = "_Fld19450",
               day_7 = "_Fld19451")
    
    tbl_store <- tbl(con_db, "_Reference156") %>% 
        select("_IDRRef", store_id = "_Code")
    
    delivery_schedule <- tbl_delivery_schedule %>%
        left_join(select(tbl_store,  "_IDRRef", recipient_store = store_id),
                  by = c(link_recipient_store = "_IDRRef")) %>% 
        left_join(select(tbl_store,  "_IDRRef", distribution_store = store_id),
                  by = c(link_distribution_store = "_IDRRef")) %>%
        group_by(recipient_store, distribution_store) %>% 
        filter(date == max(date)) %>%
        ungroup() %>% 
        select(date, recipient_store, distribution_store,
               day_1, day_2, day_3, day_4, day_5, day_6, day_7) %>%
        collect()
    
    return(delivery_schedule)

}


# Рух по складу
get_stock_motion <- function(store, date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)

    tbl_stock_movement <- tbl(con_db,"_AccumRg17702") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_recorder = "_RecorderRRef",
               link_store = "_Fld17703RRef", link_item = "_Fld17704RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17708") %>%
        mutate(active = as.logical(active)) %>%
        filter(active == "TRUE",
               date_movement >= date_start,
               date_movement <  date_finish)
    
    tbl_item <- tbl(con_db,"_Reference120") %>% 
        select("_IDRRef", "_ParentIDRRef", item_id = "_Code")
    tbl_store <- tbl(con_db, "_Reference156") %>% 
        select("_IDRRef", store_name = "_Description", store_id = "_Code")
    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code")
    tbl_sale_doc <- tbl(con_db,"_Document375") %>% 
        select("_IDRRef", nmbr_sale_doc = "_Number",
               date_sale_doc = "_Date_Time",
               link_sale_subdiv = "_Fld10904RRef")
    tbl_moving_out <- tbl(con_db,"_Document411") %>% 
        select("_IDRRef", nmbr_moving_out = "_Number",
               date_moving_out = "_Date_Time",
               link_store_receiver = "_Fld12657RRef")
    tbl_checks_report <- tbl(con_db, "_Document329") %>% 
        select("_IDRRef", date_checks_report = "_Date_Time",
               nmbr_checks_report = "_Number")
    
    df_stock_motion <- tbl_stock_movement %>%
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        filter(store_id %in% store) %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>% 
        # left_join(select(tbl_item, "_IDRRef", parent_item_id = item_id),
        #           by = c("_ParentIDRRef" = "_IDRRef")) %>%
        # filter(!parent_item_id %in% c("000500558",       # Бухгалтерские
        #                               "000300000",        # Торгів.обладнання
        #                               "000400010")) %>%   # Рекламні материали
        left_join(tbl_sale_doc ,  by = c(link_recorder = "_IDRRef")) %>% 
        left_join(tbl_subdiv,     by = c(link_sale_subdiv = "_IDRRef")) %>%
        left_join(tbl_moving_out, by = c(link_recorder = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", store_receiver = store_id),
                                  by = c(link_store_receiver = "_IDRRef")) %>%
        left_join(tbl_checks_report, by = c(link_recorder = "_IDRRef")) %>% 
        select(date_movement, store_id, type_motion, 
               nmbr_sale_doc, date_sale_doc, subdiv_id,
               nmbr_moving_out, date_moving_out, store_receiver,
               nmbr_checks_report, date_checks_report,
               item_id, item_motion_qty) %>% 
        collect() %>% 
        remove_unnecessary_items()
    
    return(df_stock_motion)

}


## Сальдо на дату по певному рахунку Корвет
account_balance <- function(date_finish, account) {

    date_finish <- as_datetime(date_finish) + months(1)
    
    tbl_acc_credit <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountCtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(date < date_finish,
               active == "TRUE")
    tbl_acc_debet <- tbl(con_db, "_AccRg855") %>% 
        select("_AccountDtRRef", date = "_Period", active = "_Active",
               link_organization = "_Fld856RRef", moving_sum = "_Fld859") %>%
        mutate(active = as.logical(active)) %>%
        filter(date < date_finish,
               active == "TRUE")
    tbl_account <- tbl(con_db, "_Acc18") %>% 
        select("_IDRRef", acc_id = "_Code")
    tbl_organization <- tbl(con_db, "_Reference128") %>% 
        select("_IDRRef", organization_id = "_Code")
    
    acc_ct <- tbl_acc_credit %>% 
        left_join(tbl_account,      by = c("_AccountCtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull
    
    acc_dt <- tbl_acc_debet %>% 
        left_join(tbl_account,      by = c("_AccountDtRRef" = "_IDRRef")) %>% 
        left_join(tbl_organization, by = c(link_organization = "_IDRRef")) %>%
        filter(organization_id == "000000001",
               acc_id == account) %>% 
        summarise(ct_sum = sum(moving_sum)) %>% 
        collect() %>% 
        pull()
    
    balance <- acc_dt - acc_ct

    return(balance)

}


## План продаж
get_sale_plan <- function(date_start, date_finish) {

    date_start <- as_datetime(date_start)
    date_finish <- as_datetime(date_finish)
    
    tbl_saleplan_doc <- tbl(con_db,"_Document350") %>% 
        select("_IDRRef", posted_saleplan_doc = "_Posted",
               date_saleplan_doc = "_Fld8946",
               link_subdiv = "_Fld8951RRef", link_project = "_Fld8952RRef", 
               link_script = "_Fld8953RRef", saleplan_doc_sum = "_Fld8954") %>% 
        mutate(posted_saleplan_doc = as.logical(posted_saleplan_doc)) %>% 
        filter(date_saleplan_doc >= date_start,
               date_saleplan_doc < date_finish,
               posted_saleplan_doc == "TRUE")

    tbl_subdiv <- tbl(con_db, "_Reference135") %>% 
        select("_IDRRef", subdiv_id = "_Code", subdiv_name = "_Description")    
    tbl_project <- tbl(con_db, "_Reference143") %>% 
        select("_IDRRef", project = "_Description")
    tbl_script <- tbl(con_db,"_Reference178") %>% 
        select("_IDRRef", script_name = "_Description")

    df_saleplan <- tbl_saleplan_doc %>%
        left_join(tbl_subdiv,         by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_project,        by = c(link_project = "_IDRRef")) %>%
        left_join(tbl_script,         by = c(link_script = "_IDRRef")) %>%
        select(date_saleplan_doc, subdiv_id, subdiv_name, project, script_name,
               saleplan_doc_sum
               
        ) %>% 
        collect()

    return(df_saleplan)
    
}


get_current_inventory <- function() {

    date_today <- as_datetime(today() + years(2000))
    
    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")

    
    tbl_stock_balance <- tbl(con_db,"_AccumRgT17710") %>% 
        select(date_balance = "_Period", link_store = "_Fld17703RRef",
               link_item = "_Fld17704RRef", item_store_qty = "_Fld17708"
        ) %>% 
        filter(date_balance > date_today) %>% 
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084")) %>%    # Обладнання

        group_by(store_id, item_id) %>%
        summarise(item_store_qty = sum(item_store_qty)) %>%
        ungroup() %>%
        filter(item_store_qty != 0) %>%
        collect()

    return(tbl_stock_balance)
    
}


get_current_inventory_batch <- function() {

    date_today <- as_datetime(today() + years(2000))
    
    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_complete_set_doc <- tbl(con_db, "_Document298") %>%
        select("_IDRRef", date_compl_set_doc = "_Date_Time")
    tbl_receipt_doc <- tbl(con_db, "_Document360") %>%
        select("_IDRRef", date_receipt_doc = "_Date_Time")

    
    tbl_stock_balance <- tbl(con_db,"_AccumRgT17307") %>% 
        select(date_balance = "_Period", link_store = "_Fld17288RRef",
               link_item = "_Fld17287RRef", item_store_qty = "_Fld17295",
               item_store_sum = "_Fld17296", item_store_vat = "_Fld17297",
               link_receipt_doc = "_Fld17291_RRRef"
        ) %>% 
        filter(date_balance > date_today) %>% 
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>% 
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084")) %>%    # Обладнання
        
        mutate(item_store_vat = ifelse(
            item_store_vat < floor((item_store_sum / 5) * 10)/10,
            0,
            item_store_vat),
            item_store_cost = item_store_sum + item_store_vat,
            ) %>% 
        
        left_join(tbl_receipt_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        left_join(tbl_complete_set_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        
        mutate(date_receipt = ifelse(!is.na(date_receipt_doc),
                                     date_receipt_doc,
                                     date_compl_set_doc)) %>% 
        
        group_by(store_id, item_id, date_receipt) %>%
        summarise(item_store_qty = sum(item_store_qty),
                  item_store_sum= sum(item_store_cost)) %>%
        ungroup() %>%
        #filter(item_store_qty != 0) %>%

        collect() %>% 
        mutate(date_receipt = as_date(date_receipt) - years(2000))
    
    return(tbl_stock_balance)

}


get_inventory_movement <- function(date_start) {

    date_start <- as_datetime(date_start + years(2000))
    
    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    
    
    tbl_stock_movement <- tbl(con_db,"_AccumRg17702") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17703RRef", link_item = "_Fld17704RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17708"
        ) %>%
        mutate(active = as.logical(active)) %>%
        filter(date_movement >= date_start,
               active == "TRUE") %>% 
        
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>%
        
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084")) %>%    # Обладнання

        mutate(date_movement = DATEADD(sql("month"),
                                       DATEDIFF(sql("month"), 0, date_movement),
                                       0)) %>%

        mutate(item_motion_qty = ifelse(type_motion == 0,
                                        item_motion_qty,
                                        -item_motion_qty)
        ) %>%

        group_by(date_movement, store_id, item_id) %>%
        summarise(store_item_moving_qty = sum(item_motion_qty, na.rm = TRUE)
        ) %>%
        ungroup() %>%
        filter(store_item_moving_qty != 0) %>%
        collect() %>% 
        mutate(date_movement = date_movement - years(2000))

    return(tbl_stock_movement)
    
}


get_inventory_movement_batch <- function(date_start) {

    date_start <- as_datetime(date_start) + years(2000)

    tbl_store <- tbl(con_db, "_Reference156") %>%
        select("_IDRRef", link_parent_store = "_ParentIDRRef", marked = "_Marked",
               store_id = "_Code") %>% 
        mutate(marked = as.logical(marked))
    tbl_items <- tbl(con_db, "_Reference120") %>% 
        select("_IDRRef", item_id = "_Code", "_ParentIDRRef")
    tbl_complete_set_doc <- tbl(con_db, "_Document298") %>%
        select("_IDRRef", date_compl_set_doc = "_Date_Time")
    tbl_receipt_doc <- tbl(con_db, "_Document360") %>%
        select("_IDRRef", date_receipt_doc = "_Date_Time")
    
    tbl_stock_movement <- tbl(con_db,"_AccumRg17286") %>% 
        select(date_movement = "_Period", active = "_Active",
               link_store = "_Fld17288RRef", link_item = "_Fld17287RRef",
               type_motion = "_RecordKind", item_motion_qty = "_Fld17295",
               item_store_sum ="_Fld17296", item_store_vat ="_Fld17297",
               link_receipt_doc = "_Fld17291_RRRef"
        ) %>%
        mutate(active = as.logical(active)) %>%
        filter(date_movement >= date_start,
               active == "TRUE") %>% 
        
        left_join(tbl_items, by = c(link_item = "_IDRRef")) %>%
        
        left_join(tbl_store, by = c(link_store = "_IDRRef")) %>% 
        left_join(select(tbl_store, "_IDRRef", parent_store = store_id),
                  by = c(link_parent_store = "_IDRRef")) %>%
        filter(!parent_store %in% c("000001096",         # Інші закриті
                                    "000001084"),        # Обладнання
               ) %>%
        
        left_join(tbl_receipt_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        left_join(tbl_complete_set_doc, by = c(link_receipt_doc = "_IDRRef")) %>%
        
        mutate(date_receipt = ifelse(!is.na(date_receipt_doc),
                                     date_receipt_doc,
                                     date_compl_set_doc)) %>% 
        
        mutate(date_movement = DATEADD(sql("month"),
                                       DATEDIFF(sql("month"), 0, date_movement),
                                       0)) %>%
        
        mutate(item_store_vat = ifelse(
                   item_store_vat < floor((item_store_sum / 5) * 10)/10,
                   0,
                   item_store_vat),
               item_store_cost = item_store_sum + item_store_vat
            ) %>%
        
        mutate(item_motion_qty = ifelse(type_motion == 0,
                                        item_motion_qty,
                                        -item_motion_qty),
               item_motion_cost = ifelse(type_motion == 0,
                                         item_store_cost,
                                         -item_store_cost)
            ) %>% 
        
        group_by(date_movement, date_receipt, store_id, item_id) %>% 
        summarise(item_motion_qty = sum(item_motion_qty),
                  item_motion_cost = sum(item_motion_cost)) %>% 
        ungroup() %>% 
        filter(item_motion_qty != 0) %>%
        
        collect() %>%
        
        mutate(date_movement = date_movement - years(2000),
               date_receipt = as_date(date_receipt) - years(2000))

    return(tbl_stock_movement)
    
}


## Ціни номенклатури (Закупочні)
price_items_data <- function(date_start, price_type = "000000005") {

    date_start <- as_datetime(date_start) + years(2000)

    tbl_price_setting <- tbl(con_db,"_InfoRg16225") %>% 
        select(date_setting = "_Period",
               link_priceType = "_Fld16226RRef", link_item = "_Fld16227RRef",
               active = "_Active", item_price = "_Fld16230") %>% 
        mutate(active = as.logical(active)) %>% 
        filter(active == "TRUE")
    
    tbl_item <- tbl(con_db,"_Reference120") %>% 
        select("_IDRRef", item_id = "_Code")
    tbl_priceType <- tbl(con_db, "_Reference183") %>% 
        select("_IDRRef", priceType_id = "_Code")
    
    df_price_item <- tbl_price_setting %>%
        left_join(tbl_priceType, by = c(link_priceType = "_IDRRef")) %>% 
        filter(priceType_id == price_type) %>% 
        left_join(tbl_item,  by = c(link_item = "_IDRRef")) %>% 
        select(date_setting, item_id, item_price) %>%
        group_by(item_id) %>%
        filter(date_setting >= max(date_setting[date_setting < date_start])) %>%
        ungroup() %>%
        collect()
    
    return(df_price_item)

}


get_item_main_parent <- function(ref_df) {
    
    item_df <- ref_df[,1]
        
    current_rec <- left_join(item_df,
                             select(ref_df, item_id, parent_last = parent_itemId)) 
    i <- 1
    row_qty <- nrow(current_rec)
    while(sum(is.na(current_rec[, ncol(current_rec)])) != row_qty) {
        prev_parent <- current_rec
        current_rec <- left_join(prev_parent,
                                 select(ref_df, item_id, parent_itemId),
                                 by = c("parent_last" = "item_id"))
        names(current_rec)[ncol(current_rec) - 1] <- stringr::str_glue("parent_level_{i}")
        names(current_rec)[ncol(current_rec)] <- "parent_last"
        i <- i + 1
    }
    
    res <- current_rec %>% 
        mutate(main_parent = coalesce(!!!rev(select(., -item_id)))) %>% 
        # mutate(last_non_na = max.col(!is.na(current_rec), ties.method = "last")) %>%
        select(main_parent)
    
    return(res)
    
}


get_references <- function() {

    tbl_item <- tbl(con_db, "_Reference120") %>%
        select(
            "_IDRRef",
            item_id = "_Code",
            item_name = "_Description",
            pack_qty = "_Fld20858",
            "_Fld2048RRef",
            "_Fld2045RRef",
            "_ParentIDRRef",
            "_Fld2060RRef",
            is_tva = "_Fld2068",
            item_marked = "_Marked",
            link_unit_size = "_Fld2039RRef"
        ) %>%
        mutate(is_tva = as.logical(is_tva),
               item_marked = as.logical(item_marked))
    
    tbl_price_group <- tbl(con_db, "_Reference235") %>%
        select("_IDRRef", price_group = "_Description")
    
    tbl_group <- tbl(con_db, "_Reference122") %>%
        select(
            "_IDRRef",
            group_id = "_Code",
            group_name = "_Description",
            "_Fld19599RRef",
            "_Fld18403RRef",
            "_ParentIDRRef",
            group_marked = "_Marked"
        ) %>%
        mutate(group_marked = as.logical(group_marked))
    
    tbl_unit_size <- tbl(con_db, "_Reference84") %>%
        select(
            "_IDRRef",
            width = "_Fld18500",
            height = "_Fld18501",
            depth = "_Fld18499",
            weight = "_Fld1642",
            volume = "_Fld1643"
        )
    
    ref_items <- tbl_item %>%
        left_join(
            select(tbl_item, "_IDRRef", parent_itemId = item_id),
            by = c("_ParentIDRRef" = "_IDRRef")
        ) %>%
        left_join(tbl_price_group, by = c("_Fld2060RRef" = "_IDRRef")) %>%
        left_join(tbl_group, by = c("_Fld2045RRef" = "_IDRRef")) %>%
        left_join(tbl_unit_size, by = c(link_unit_size = "_IDRRef")) %>%
        select(
            item_id,
            item_name,
            price_group,
            group_id,
            pack_qty,
            is_tva,
            item_marked,
            parent_itemId,
            width,
            height,
            depth,
            weight,
            volume
        ) %>%
        collect()
    

    ref_items <<- ref_items %>% 
        bind_cols(get_item_main_parent(.))
    
    tbl_category <- tbl(con_db, "_Reference19598") %>%
        select("_IDRRef", is_marked = "_Marked",
               category_id = "_Code", category_name = "_Description") %>% 
        mutate(is_marked = as.logical(is_marked))
    
    ref_item_category <<- tbl_category %>% 
        select(category_id, category_name, is_marked) %>% 
        collect() %>% 
        mutate(category_name = case_when(
            category_id %in% "000000003"  ~ "07. Приладдя д/письма",
            category_id %in% "000000005"  ~ "06. Усп.керівник",
            category_id %in% "000000011"  ~ "04. Прибирання, кава",
            category_id %in% "000000020"  ~ "16. Лайфстайл",
            .default = category_name            
        ))
    
    ref_item_group <<- tbl_group %>%
        left_join(
            select(tbl_group, "_IDRRef", parent_group = group_id),
            by = c("_ParentIDRRef" = "_IDRRef")
        ) %>%
        left_join(tbl_category, by = c("_Fld19599RRef" = "_IDRRef")) %>%
        select(group_marked,
               group_id,
               group_name,
               parent_group,
               category_name,
               category_id) %>%
        collect()
    
    tbl_subdiv <- tbl(con_db, "_Reference135") %>%
        select(
            "_IDRRef",
            subdiv_id = "_Code",
            subdiv_name = "_Description",
            "_ParentIDRRef",
            marked = "_Marked",
            link_store = "_Fld18502RRef"
        ) %>%
        mutate(marked = as.logical(marked))

    tbl_store <- tbl(con_db, "_Reference156") %>%
        select(
            "_IDRRef",
            store_id = "_Code",
            store_name = "_Description",
            store_location = "_Fld2312",
            link_subdiv = "_Fld2296RRef",
            link_distribution_store = "_Fld19506RRef",
            marked = "_Marked",
            link_parent_store = "_ParentIDRRef"
        ) %>%
        mutate(marked = as.logical(marked))
    
    ref_subdiv <<- tbl_subdiv %>%
        left_join(select(tbl_subdiv, "_IDRRef", parent = subdiv_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", store_id),
                  by = c(link_store = "_IDRRef")) %>%
        select(subdiv_id, subdiv_name, parent, store_id, marked) %>%
        collect()

    
    tbl_locality <- tbl(con_db, "_Reference199") %>%
        select("_IDRRef", locality_id = "_Code")
    tbl_region <- tbl(con_db, "_Reference149") %>%
        select("_IDRRef", region_id = "_Code")
    
    tbl_delivery_address <- tbl(con_db, "_Reference194") %>%
        select(
            marked = "_Marked",
            is_main = "_Fld2740",
            link_owner = "_OwnerID_RRRef",
            link_locality = "_Fld18128RRef",
            link_region = "_Fld2738RRef",
            long = "_Fld24539",
            lat = "_Fld24538"
        ) %>%
        mutate(marked  = as.logical(marked),
               is_main = as.logical(is_main)) %>%
        filter(marked == "FALSE",
               is_main == "TRUE") %>%
        left_join(select(tbl_store, "_IDRRef", store_id),
                  by = c(link_owner = "_IDRRef")) %>%
        left_join(tbl_locality,
                  by = c(link_locality = "_IDRRef")) %>%
        left_join(tbl_region,
                  by = c(link_region = "_IDRRef")) %>%
        select(store_id, locality_id, region_id, lat, long) %>%
        filter(!is.na(store_id))
    
    ref_store <<- tbl_store %>%
        left_join(
            select(tbl_store, "_IDRRef", distribution_store = store_id),
            by = c(link_distribution_store = "_IDRRef")
        ) %>%
        left_join(
            select(tbl_store, "_IDRRef", parent_id = store_id),
            by = c(link_parent_store = "_IDRRef")
        ) %>% 
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_id),
                  by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_delivery_address, by = "store_id") %>%
        select(
            store_id,
            parent_id,
            marked,
            distribution_store,
            subdiv_id,
            locality_id,
            region_id,
            lat,
            long,
            store_name,
            store_location
        ) %>%
        collect()
    
    
    tbl_area <- tbl(con_db, "_InfoRg22018") %>%
        select(
            date = "_Period",
            active = "_Active",
            link_subdiv = "_Fld22019RRef",
            link_type_area = "_Fld22020RRef",
            area_value = "_Fld22021"
        ) %>%
        mutate(active = as.logical(active)) %>%
        filter(active == "TRUE")
    
    tbl_type_area <- tbl(con_db, "_Enum22007") %>%
        select("_IDRRef", type_area = "_EnumOrder")
    
    ref_shop_area <<- tbl_area %>%
        left_join(tbl_subdiv,    by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_type_area, by = c(link_type_area = "_IDRRef")) %>%
        select(date, subdiv_id, type_area, area_value) %>%
        collect() %>%
        mutate(
            type_area = case_when(
                type_area == 0 ~ "total_area",
                type_area == 1 ~ "trade_area",
                type_area == 2 ~ "storage",
                type_area == 3 ~ "b2b_area",
                type_area == 4 ~ "copycenter_area"
            )
        )
    

    tbl_expenses_items <- tbl(con_db, "_Reference169") %>%
        select(
            "_IDRRef",
            expItems_id = "_Code",
            expItems_name = "_Description",
            "_ParentIDRRef",
            expItems_marked = "_Marked"
        ) %>%
        mutate(expItems_marked = as.logical(expItems_marked))
    
    ref_expenses <<- tbl_expenses_items %>%
        left_join(
            select(tbl_expenses_items, "_IDRRef",
                   expItems_parent = expItems_name),
            by = c("_ParentIDRRef" = "_IDRRef")
        ) %>%
        select(expItems_id, expItems_name, expItems_parent, expItems_marked) %>%
        distinct() %>%
        collect()
    
    
    tbl_route <- tbl(con_db, "_Reference19642") %>%
        select(
            "_IDRRef",
            "_ParentIDRRef",
            route_id = "_Code",
            link_subdiv = "_Fld19647RRef",
            route_marked = "_Marked",
            route = "_Description"
        ) %>%
        mutate(route_marked = as.logical(route_marked))
    tbl_route_regions <- tbl(con_db, "_Reference19642_VT19654") %>%
        select(
            "_Reference19642_IDRRef",
            link_region = "_Fld19656RRef")
    
    ref_route <<- tbl_route %>%
        left_join(select(tbl_route, "_IDRRef",
                         route_parent = route_id),
                  by = c("_ParentIDRRef" = "_IDRRef")) %>%
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_id),
                  by = c(link_subdiv = "_IDRRef")) %>%
        left_join(tbl_route_regions,
                  by = c("_IDRRef" = "_Reference19642_IDRRef")) %>%
        left_join(tbl_region,
                  by = c(link_region = "_IDRRef")) %>%
        select(route_id, route, route_parent, subdiv_id, route_marked,
               region_id) %>%
        collect() %>% 
        nest(regions = region_id)
    
    tbl_auto <- tbl(con_db, "_Reference195") %>%
        select(
            auto_id = "_Code",
            auto_name = "_Description",
            is_cargo = "_Fld21584",
            marked = "_Marked",
            link_subdiv = "_Fld19665RRef",
            link_store = "_Fld19668RRef",
            tank = "_Fld21651",
            load_capacity = "_Fld19666",
            volume = "_Fld19667"
        ) %>%
        mutate(marked = as.logical(marked),
               is_cargo = as.logical(is_cargo))
    ref_auto <<- tbl_auto %>%
        left_join(select(tbl_subdiv, "_IDRRef", subdiv_id),
                  by = c(link_subdiv = "_IDRRef")) %>%
        left_join(select(tbl_store, "_IDRRef", store_id),
                  by = c(link_store = "_IDRRef")) %>%
        select(auto_id,
               auto_name,
               marked,
               subdiv_id,
               store_id,
               is_cargo,
               load_capacity,
               volume,
               tank) %>%
        collect()
    
    
    tbl_object_property_value_rg <- tbl(con_db, "_InfoRg14478") %>%
        select("_Fld14479_RRRef", property = "_Fld14480RRef",
               value = "_Fld14481_RRRef")
    
    tbl_character_types <- tbl(con_db, "_Chrc806") %>%
        select("_IDRRef", code = "_Code")
    
    tbl_object_property_value_ref <- tbl(con_db, "_Reference85") %>%
        select("_IDRRef", name = "_Description")
    
    ref_subdiv_category <<- tbl_subdiv %>%
        filter(marked == "FALSE") %>%
        left_join(tbl_object_property_value_rg,
                  by = c("_IDRRef" = "_Fld14479_RRRef")) %>%
        inner_join(tbl_character_types, by = c(property = "_IDRRef")) %>%
        filter(code == "000000191") %>%
        left_join(tbl_object_property_value_ref, by = c(value = "_IDRRef")) %>%
        select(subdiv_id, subdiv_category = name) %>%
        collect()
    
}
