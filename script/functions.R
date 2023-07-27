get_shipment_share <- function(date_finish, trade_subdiv_stores) {
    
    shipment_share <- get_stock_motion(trade_subdiv_stores,
                                       date_finish - months(3) + years(2000),
                                       date_finish + years(2000)) %>% 
        filter(type_motion == 1,
               !is.na(nmbr_sale_doc) | !is.na(nmbr_checks_report)) %>% 
        left_join(select(ref_subdiv, subdiv_id, subdiv_store = store_id),
                  by = "subdiv_id") %>% 
        filter(is.na(subdiv_id) | store_id == subdiv_store) %>% 
        left_join(select(ref_store, store_subdiv = subdiv_id, store_id),
                  by = "store_id") %>%
        mutate(subdiv_id = store_subdiv,
               type_subdiv = case_when(
                   !is.na(nmbr_sale_doc)      ~ "service",
                   !is.na(nmbr_checks_report) ~ "retail")) %>% 
        group_by(subdiv_id, type_subdiv) %>% 
        summarise(total_type_units = sum(item_motion_qty)) %>% 
        group_by(subdiv_id) %>% 
        mutate(avr_subdiv_share = round(total_type_units /sum(total_type_units),
                                        3)) %>% 
        select(-total_type_units)
}

get_expenses_data <- function(date_finish) {
    
    expenses_data <- get_expenses(date_finish - months(12) + years(2000),
                                     date_finish + years(2000)) %>% 
        
        filter(expItems_id != "100000421") %>% 
        mutate(subdiv_id = ifelse(subdiv_id %in% "000000189",   # Копіцентр ХМ СМ
                                  "000000007",
                                  subdiv_id)) %>%
        mutate(date = floor_date(date - years(2000), unit = "month"),
               exp_sum = ifelse(type_exp == 0,
                                exp_sum + exp_vat,
                                -(exp_sum + exp_vat)),
               ) %>% 
        select(-exp_vat, -type_exp) %>% 
        left_join(select(ref_expenses, expItems_id, expItems_name, expItems_parent),
                  by = c("expItems_id", "expItems_name")) %>% 
        left_join(select(ref_expenses, expItems_name, parent_2 = expItems_parent),
                  by = c("expItems_parent" = "expItems_name")) %>% 
        mutate(expenses_item = ifelse(is.na(parent_2),
                                      expItems_parent,
                                      parent_2)) %>%
        filter(!expenses_item %in% c("53 Курсові різниці",
                                     "44 Спонсорська/матеріальна допомога",
                                     "43 Капітальні інвестиції",
                                     "41 Ремонти")) %>% 
        mutate(subdiv_id = ifelse(subdiv_id %in% c("000000002", # Бухгалт.
                                                   "000000025", # АСУ
                                                   "000000027", # Служба енергет.
                                                   "000000029", # Кадри
                                                   "000000031", # Відділ закупок
                                                   "000000037", # Виробничий відділ
                                                   "000000088", # Адмін.-господарський відділ
                                                   "000000091", # Відділ КРУ
                                                   "000000124", # Відділ маркетингу і реклами
                                                   "000000128", # Служба безпеки
                                                   "000000131", # Адміністрація Корвету
                                                   "000000143", # Служба ОП
                                                   "000000146", # Відділ регіонального розвитку
                                                   "000000147", # Відділ корпоративного продажу в регіоні
                                                   "000000158", # Заправка картріджів
                                                   "000000161", # Відділ БМ
                                                   "000000163", # В декреті
                                                   "000000164", # Відділ ТАГЗ
                                                   "000000165", # Управління торгівлі
                                                   "000000166", # Виробництво
                                                   "000000205", # Відділ управління персоналом
                                                   "000000206", # Служба з охорони та безпеки
                                                   "000000209", # Операційно-аналітичний відділ
                                                   "000000210", # Відділ категорійного менеджменту
                                                   "000000215", # Департамент корпоративного продажу
                                                   "000000216", # Відділ маркетингу
                                                   "000000230", # Департамент роздрібного продажу
                                                   "000000236", # PROM
                                                   "000000244", # Департамент регіонального розвитку
                                                   "000000249"  # Тендерний відділ
        ),
        "000000001",
        subdiv_id),
        subdiv_id = ifelse(subdiv_id %in% "000000189",   # Копіцентр ХМ СМ
                           "000000007",
                           subdiv_id),
        subdiv_id = ifelse(subdiv_id %in% "000000251",   # Копіцентр Луцьк СМ
                           "000000190",
                           subdiv_id),
        subdiv_id = ifelse(subdiv_id %in% "000000256",   # Pено Мастер ВХ51-02 НА
                           "000000257",
                           subdiv_id)
        )
    
}


get_salary_data <- function(date_finish) {
    
    compensation_data <- get_compensation(date_finish - months(3)+ years(2000),
                                          date_finish + years(2000)) %>% 
        mutate(date = date - years(2000)) %>% 
        mutate(total_salary = -total_salary_expenses) %>% 
        select(date, subdiv_id, position_id, individ_id, total_salary) %>% 
        mutate(subdiv_id = ifelse(subdiv_id %in% "000000189",   # Копіцентр ХМ СМ
                                  "000000007",
                                  subdiv_id))
    
    salary_data <- tibble(date_start = seq.Date(from = date_finish - months(3)+
                                                    years(2000),
                                                to   = date_finish - months(1)+
                                                    years(2000),
                                                by   = "month"),
                          date_finish = seq.Date(from = date_finish - months(2)+
                                                     years(2000),
                                                 to   = date_finish +
                                                     years(2000),
                                                 by   = "month")) %>% 
        mutate(data = map2(date_start, date_finish, salary_fn)) %>%
        mutate(date = date_start - years(2000)) %>% 
        select(-date_start, -date_finish) %>% 
        unnest(data) %>% 
        bind_rows(compensation_data) %>% 
        group_by(date, subdiv_id, position_id, individ_id) %>% 
        summarise(total_salary = sum(total_salary, na.rm = TRUE),
                  stuff_unit = sum(stuff_unit, na.rm = TRUE),
                  working_hours = sum(working_hours, na.rm = TRUE)) %>% 
        ungroup()
    
}


get_direct_costs <- function(expenses_data, salary_data,
                             date_finish, trade_subdiv) {
    
    # marketing_expenses <- expenses_data %>% 
    #     filter(date >= date_finish - months(3),
    #            expenses_item %in% "185 Маркетингові витрати") %>% 
    #     group_by(date, subdiv_id) %>% 
    #     summarise(exp_sum = sum(exp_sum)) %>%
    #     group_by(subdiv_id) %>% 
    #     summarise(marketing_exp_sum = round(mean(exp_sum)))
    
    utilities_data <- expenses_data %>% 
        filter(date >= date_finish - months(3),
               expenses_item %in% c("04 Комунальні послуги",
                                    "091 Генератори, інш оствітл та обладнання")) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(exp_sum = sum(exp_sum)) %>%
        group_by(subdiv_id) %>% 
        summarise(utilities_exp_sum = round(mean(exp_sum)))
    
    rent_data <- expenses_data %>% 
        filter(date >= date_finish - months(3),
               expenses_item %in% "02 Орендна плата") %>% 
        mutate(subdiv_id = ifelse(subdiv_id %in% "000000227", # Склад Івано-Франківськ
                                  "000000225",                # Івано-Франківськ см сервіс
                                  subdiv_id)) %>%
        group_by(date, subdiv_id) %>% 
        summarise(exp_sum = sum(exp_sum)) %>%
        group_by(subdiv_id) %>% 
        summarise(avr_rent_sum = round(max(exp_sum)))
    
    
    salary_logistic_subdiv <- salary_data %>% 
        filter(subdiv_id %in% c("000000004",     # Відділ складської логістики
                                "000000197")     # Логістика, Луцьк
        ) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(total_salary = sum(total_salary)) %>% 
        ungroup() %>% 
        group_by(subdiv_id) %>% 
        summarise(avr_salary_sum = round(mean(total_salary)))
    
    salary_retail_main <- salary_data %>% 
        filter(subdiv_id %in% trade_subdiv,
               position_id %in% c("000000031",     # Керуючий магазином
                                  "000000032",     # Заст.керуючого магазином
                                  "000000033",     # Продавець
                                  "000000043",     # Комірник
                                  "000000187",     # Оператор копіюв.машин
                                  "000000203",     # Адміністратор залу
                                  "000000012"      # Контролер-ревізор
                                  ),
               !is.na(subdiv_id)) %>% 
        left_join(ref_subdiv_category, by = "subdiv_id") %>% 
        mutate(total_salary = case_when(
                   subdiv_id %in% "000000140" &           # Чернівці
                        position_id %in% "000000031"     ~ 0,
                   position_id %in% "000000031"         ~ total_salary * 0.15,
                   .default = total_salary
               ),
               stuff_unit = case_when(
                   subdiv_id %in% "000000140" &           # Чернівці
                       position_id %in% "000000031"     ~ 0,
                   position_id %in% "000000031"         ~ stuff_unit * 0.15,
                   .default = stuff_unit
               ),
               working_hours = case_when(
                   subdiv_id %in% "000000140" &           # Чернівці
                       position_id %in% "000000031"     ~ 0,
                   position_id %in% "000000031"         ~ working_hours * 0.15,
                   .default = working_hours
               )
        ) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(total_salary = sum(total_salary),
                  stuff_units = round(sum(stuff_unit, na.rm = TRUE), 1),
                  total_working_hours = sum(working_hours, na.rm = TRUE)
                  ) %>% 
        group_by(subdiv_id) %>% 
        summarise(avr_salary_sum = round(mean(total_salary)),
                  stuff_units = last(stuff_units),
                  avr_working_hours = round(mean(total_working_hours))) %>% 
        bind_rows(salary_logistic_subdiv)
    
    salary_retail_supporting <- salary_data %>% 
        filter(subdiv_id %in% trade_subdiv,
               position_id %in% c("000000036",     # Прибиральник служб.приміщень
                                  "000000049"      # Оператор котельні
        ),
        !is.na(subdiv_id)) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(exp_sum = sum(total_salary)) %>% 
        ungroup() %>% 
        mutate(expenses_item = "Комунальні послуги - з/п персоналу")
    
    direct_costs <- expenses_data %>% 
        filter(date >= date_finish - months(3)) %>% 
        group_by(date, subdiv_id, expenses_item) %>% 
        summarise(exp_sum = sum(exp_sum)) %>% 
        filter(!expenses_item %in% c("00 Заробітна плата",
                                     "02 Орендна плата",
                                     "04 Комунальні послуги",
                                     "091 Генератори, інш оствітл та обладнання",
                                     "08 Автовитрати, запчастини",
                                     "09 Витрати на пальне"
                                     #"185 Маркетингові витрати",
                                     #"29 Податок на прибуток від осн. діяльності"
        )
        ) %>% 
        mutate(subdiv_id = ifelse(subdiv_id %in% "000000227", # Склад Івано-Франківськ
                                  "000000225",                # Івано-Франківськ см сервіс
                                  subdiv_id)) %>% 
        bind_rows(salary_retail_supporting) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(exp_sum = sum(exp_sum)) %>%
        group_by(subdiv_id) %>% 
        summarise(avr_total_exp_sum = round(mean(exp_sum))) %>% 
        # left_join(select(ref_subdiv, subdiv_id, subdiv_name),
        #           by = "subdiv_id") %>% 
        left_join(salary_retail_main,
                  by = "subdiv_id") %>% 
        filter(!is.na(avr_salary_sum)) %>% 
        left_join(rent_data, by = "subdiv_id") %>% 
        left_join(utilities_data, by = "subdiv_id") # %>% 
        # left_join(marketing_expenses, by = "subdiv_id") 
}


get_lutsk_stock_motion_share <- function(distribution_stores_motion_data,
                                         lutsk_distribution_store) {
    
    lutsk_distribution_subdiv <- ref_subdiv %>% 
        filter(marked == FALSE) %>% 
        select(subdiv_id, store_id) %>% 
        inner_join(lutsk_distribution_store,
                   by = "store_id")
    
    lutsk_stock_motion_share <- distribution_stores_motion_data %>% 
        filter(store_id == "000001073",
               is.na(subdiv_id) | subdiv_id %in% lutsk_distribution_subdiv$subdiv_id,
               is.na(store_receiver) |
                   store_receiver %in% lutsk_distribution_store$store_id) %>% 
        left_join(select(ref_store, store_subdiv = subdiv_id, store_id),
                  by = c("store_receiver" = "store_id")) %>% 
        mutate(receiver = ifelse(!is.na(subdiv_id),
                                 subdiv_id,
                                 store_subdiv),
               type = ifelse(!is.na(subdiv_id),
                             "service",
                             "retail")
               ) %>% 
        group_by(receiver, type) %>% 
        summarise(total_units = sum(item_motion_qty)) %>% 
        ungroup() %>% 
        mutate(lutsk_subdiv_share = round(total_units / sum(total_units), 3)) %>% 
        filter(type == "retail") %>% 
        select(subdiv_id = receiver, lutsk_subdiv_share)
 
}   


get_main_stock_motion_share <- function(distribution_stores_motion_data,
                                        lutsk_stock_motion_share,
                                        super_shipment_share,
                                        trade_subdiv) {
    
    main_stock_motion_share <- distribution_stores_motion_data %>% 
        filter(store_id == "000000001") %>% 
        left_join(select(ref_store, store_subdiv = subdiv_id, store_id),
                  by = c("store_receiver" = "store_id")) %>% 
        mutate(store_subdiv = ifelse(store_receiver %in% "000001064", # Тернопіль Накопичувач
                                     "000000139",                     # Тернопіль см сервіс
                                     store_subdiv),
               store_subdiv = ifelse(store_receiver %in% "000001098", # ІваноФранківськ Накопичувач
                                     "000000225",                     # Івано-Франківськ см сервіс
                                     store_subdiv),
               receiver = ifelse(!is.na(subdiv_id),
                                 subdiv_id,
                                 store_subdiv),
               type = ifelse(!is.na(subdiv_id),
                             "service",
                             "retail")
               ) %>% 
        filter(receiver %in% c(trade_subdiv, "000000197")) %>% 
        group_by(receiver, type) %>% 
        summarise(total_units = sum(item_motion_qty)) %>% 
        ungroup() %>% 
        mutate(subdiv_share = round(total_units / sum(total_units), 3)) %>% 
        filter(type == "retail") %>% 
        select(subdiv_id = receiver, subdiv_share) %>% 
        
        left_join(lutsk_stock_motion_share, by = "subdiv_id") %>% 
        mutate(subdiv_share = ifelse(!is.na(lutsk_subdiv_share),
                                     subdiv_share + lutsk_subdiv_share *
                                         subdiv_share[subdiv_id == "000000197"],
                                     subdiv_share)) %>% 
        filter(!subdiv_id %in% "000000197") %>% 
        select(-lutsk_subdiv_share) %>%
        
        left_join(super_shipment_share, by = "subdiv_id") %>% 
        mutate(avr_share = ifelse(!is.na(avr_subdiv_share),
                                  subdiv_share * avr_subdiv_share,
                                  subdiv_share)) %>% 
        select(subdiv_id, avr_share)
    
} 


get_distribution_stores_cost <- function(direct_costs,
                                         salary_data,
                                         main_stock_motion_share,
                                         lutsk_stock_motion_share) {
    
    main_store_costs <- direct_costs %>% 
        filter(subdiv_id == "000000004") %>% 
        select(-stuff_units, -avr_working_hours) %>%  
        summarise(sum = sum(across(where(is.numeric)), na.rm = TRUE)) %>% 
        pull(sum)
    
    lutsk_distr_store_costs <- direct_costs %>% 
        filter(subdiv_id == "000000197") %>% 
        select(-stuff_units, -avr_working_hours) %>%  
        summarise(sum = sum(across(where(is.numeric)), na.rm = TRUE)) %>% 
        pull(sum)
    
    lutsk_transport_salary <- salary_data %>% 
        filter(position_id %in% c("000000039", "000000041"),
               subdiv_id == "000000197") %>% 
        group_by(date, subdiv_id) %>% 
        summarise(total_salary = sum(total_salary)) %>% 
        ungroup() %>% 
        summarise(round(mean(total_salary))) %>% 
        pull()
    
    distribution_stores_cost <- main_stock_motion_share %>% 
        left_join(lutsk_stock_motion_share, by = "subdiv_id") %>% 
        mutate(main_store_cost_sum = avr_share * main_store_costs,
               lutsk_distr_store_cost_sum = lutsk_subdiv_share *
                   (lutsk_distr_store_costs - lutsk_transport_salary)) %>%
        replace_na(list(main_store_cost_sum = 0,
                        lutsk_distr_store_cost_sum = 0)) %>% 
        mutate(distr_stores_cost = main_store_cost_sum +
                   lutsk_distr_store_cost_sum) %>% 
        select(subdiv_id, distr_stores_cost)
    
}


get_truck_1km_cost <- function(delivery_raw, salary_data,
                               expenses_data, date_finish) {
    
    # Розрахунок транспортної з/п по авто
    
    delivery_data <- delivery_raw %>% 
        filter(distance > 0,
               as_date(time_return) > as_date("4001-01-01")) %>% 
        distinct(nmbr_delivery, date, forwarder, driver, vehicle_id, vehicle_name,
                 direction, route_id, duration, distance) %>% 
        mutate(duration = ifelse(duration < 0,
                                 1,
                                 duration)) %>% 
        mutate(forwarder = ifelse(forwarder %in% driver,
                                  NA,
                                  forwarder))
    
    forwarders_data <- delivery_data %>% 
        filter(!is.na(forwarder)) %>%
        select(-driver) %>% 
        rename(person = forwarder)
    
    vehicle_staff_data <- delivery_data %>% 
        select(-forwarder) %>% 
        rename(person = driver) %>%
        bind_rows(forwarders_data) %>% 
        group_by(date, person, vehicle_id) %>% 
        summarise(total_duration = sum(duration)) %>% 
        left_join(select(salary_data, individ_id, date, position_id, working_hours,
                         total_salary),
                  by = c("date", "person" = "individ_id"),
                  multiple = "all") %>% 
        filter(position_id %in% c("000000041", "000000039")) %>% 
        group_by(date, person) %>% 
        mutate(person_exp = round(total_duration / sum(total_duration) * total_salary),
        ) %>% 
        group_by(date, vehicle_id) %>% 
        summarise(person_exp = sum(person_exp)) %>% 
        ungroup() %>% 
        left_join(select(ref_auto, auto_id, is_cargo, subdiv_id),
                  by = c("vehicle_id" = "auto_id")) %>% 
        filter(is_cargo,
               !vehicle_id %in% "000000035"       # Опель Віваро, №ВХ5008СВ
        ) %>% 
        select(-is_cargo)
    
    
    # Розрахунок витрат на пальне
    
    trucks_distinct <- vehicle_staff_data %>% 
        distinct(subdiv_id) %>% pull()
    
    cargo_auto_id <<- vehicle_staff_data %>% 
        distinct(vehicle_id) %>% pull()
    
    truck_fuel_data <- expenses_data %>% 
        filter(subdiv_id %in% trucks_distinct,
               expenses_item %in% c("09 Витрати на пальне"),
               date >= date_finish - months(3)) %>%
        group_by(date, subdiv_id) %>% 
        summarise(fuel_sum = sum(exp_sum)) %>% 
        ungroup()
    
    
    # Розрахунок витрат на утримання авто
    
    truck_maintenance <- expenses_data %>% 
        filter(subdiv_id %in% trucks_distinct,
               expenses_item %in% c("08 Автовитрати, запчастини",
                                    "231 Оформлення документів", "24 Страхування")
        ) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(maintenance_sum = sum(exp_sum, na.rm = TRUE)) %>% 
        group_by(subdiv_id) %>%
        summarise(avr_maintenance_sum = round(mean(maintenance_sum) / 100) * 100)
    
    
    # Розрахунок транспортних витрат на 1км
    
    truck_1km_cost <- delivery_raw %>%
        distinct(nmbr_delivery, date_delivery, date, vehicle_id, distance) %>% 
        group_by(date, vehicle_id) %>% 
        summarise(total_distance = sum(distance, na.rm = TRUE)) %>% 
        ungroup() %>% 
        right_join(vehicle_staff_data,
                   by = c("date", "vehicle_id")) %>% 
        left_join(truck_fuel_data,
                  by = c("date", "subdiv_id")) %>% 
        left_join(truck_maintenance,
                  by = "subdiv_id") %>% 
        mutate(cost_1km = round((person_exp + fuel_sum + avr_maintenance_sum) / 
                                    total_distance,
                                3)) %>% 
        select(date, vehicle_id, cost_1km)
    
}


get_trip_distance <- function(data) {
    coords <- data %>%
        sf::st_as_sf(coords = c("long", "lat"), crs = 4326)
    
    tries <- 3

    for (i in 1:tries) {
        tryCatch({
            # Відправити запит на сервер та отримати статус відповіді

            route <- osrm::osrmTrip(coords, osrm.server = "http://192.168.0.69:8040/")
            
            # Перевірити, чи було отримано маршрут
            if (!is.null(route)) {
                # Якщо все в порядку, повернути маршрут та вийти з циклу
                return(tibble(start = route[[1]]$trip$start,
                              end = route[[1]]$trip$end,
                              distance = route[[1]]$trip$distance))
            }
        }, error = function(e) {
            # Якщо виникла помилка з'єднання, вивести повідомлення та спробувати знову
            print(paste0("Помилка: ", e$message))
        })
        
        # Зачекати перед наступною спробою
        Sys.sleep(10)
    }
    
    # Якщо не вдалося отримати маршрут після трьох спроб, повернути повідомлення
    shinyalert("Проблеми з інтернетом! Додаток буде закритий!",
               callbackR = function() { session$close() })

}

get_distance <- function(data) {
    total_distance <- sum(data$distance)
    points_qty <- nrow(data) - 1
    distance_df <- tibble(pos = data$end[1:nrow(data)-1]) %>% 
        mutate(id = row_number()) %>% 
        rowwise() %>% 
        mutate(dist_for_weight= min(sum(data$distance[1:id]),
                                    sum(data$distance[(id+1):nrow(data)]))) %>% 
        ungroup() %>% 
        mutate(weight = dist_for_weight / sum(dist_for_weight),
               distance = round(weight * total_distance, 1)) %>% 
        select(pos, distance) %>% 
        arrange(pos)
    
    return(distance_df$distance)
}


get_subdiv_transport_cost <- function(delivery_raw,
                                      truck_1km_cost,
                                      lutsk_stock_motion_share,
                                      date_finish,
                                      trade_subdiv_stores,
                                      lutsk_distribution_store) {
    
    min_deliv_order_date <- delivery_raw %>% 
        filter(date_deliv_order == min(date_deliv_order, na.rm = TRUE)) %>% 
        mutate(date_deliv_order = as_date(date_deliv_order)) %>% 
        slice(1) %>% pull(date_deliv_order)
    
    delivery_order_data <- get_delivery_order_data(min_deliv_order_date,
                                                   date_finish + years(2000))   
    
    distribution_stores <- unique(na.omit(ref_store$distribution_store))
    
    
    delivery_to_trade_store <- delivery_raw %>%
        filter(!direction %in% c("САМОВИВІЗ", "Закриття документів"),
               !route_id %in% "000000106",           # Інтернет магазин
               !is.na(nmbr_deliv_order),
               to_intermediate == FALSE,
               speedo_finish > 0 & speedo_finish > speedo_start,
               vehicle_id %in% cargo_auto_id,
        ) %>%
        select(date_delivery, nmbr_delivery, nmbr_deliv_order, date_deliv_order,
               vehicle_id, route_id, delivery_store = store_id,
               speedo_start, speedo_finish) %>% 
        left_join(select(delivery_order_data, nmbr_deliv_order, date_deliv_order,
                         address_region, recipient_store),
                  by = c("nmbr_deliv_order", "date_deliv_order")) %>% 
        left_join(select(ref_route, route_id, route_parent, regions),
                  by = "route_id") %>%
        rowwise() %>% 
        filter(!is.na(recipient_store),
               recipient_store %in% trade_subdiv_stores,  
               delivery_store %in% distribution_stores |
                   (delivery_store == "000000022" &
                        recipient_store == "000001110"),    #  Кременець
               address_region %in% regions[[1]]) %>% 
        ungroup() 
    
    
    deliveries_summary <- delivery_to_trade_store %>% 
        mutate(recipient_store = ifelse(
            delivery_store == "000000001" &
                recipient_store %in% lutsk_distribution_store$store_id,
            "000001073",
            recipient_store)) %>% 
        filter(!(delivery_store == "000001073" &
                     recipient_store == "000001111"),  # Луцьк Ковельська
               !(delivery_store == "000000001" &
                     recipient_store == "000001110")   # Кременець
               )%>% 
        group_by(nmbr_delivery, date_delivery, route_parent, delivery_store,
                 vehicle_id) %>%
        distinct(recipient_store) %>% 
        arrange(recipient_store, .by_group = TRUE) %>% 
        mutate(store_list = toString(str_sort(unique(na.omit(recipient_store))))
        ) %>% 
        ungroup()
    
    distinct_distance_data <- deliveries_summary %>% 
        distinct(delivery_store, store_list) %>% 
        splitstackshape::cSplit(splitCols = "store_list", ", ", type.convert = FALSE,
                                drop = FALSE) %>% 
        mutate(id = row_number())
    
    store_distance_raw <- distinct_distance_data %>%     
        select(id, delivery_store, starts_with("store_list_")) %>% 
        pivot_longer(cols = contains("store"), names_to = "store") %>%
        filter(!is.na(value)) %>% 
        left_join(select(ref_store, store_id, lat, long),
                  by = c("value" = "store_id"))
    
    store_distance_data <- store_distance_raw %>% 
        group_by(id) %>% 
        nest() %>% 
        mutate(distance_data = map(data, get_trip_distance)) %>% 
        transmute(distance = map(distance_data, get_distance)) %>% 
        unnest(col = distance) %>% 
        ungroup()
    
    store_distance <- store_distance_raw %>% 
        filter(store != "delivery_store") %>% 
        bind_cols(select(store_distance_data, distance)) %>% 
        select(id, recipient_store = value, distance) %>% 
        left_join(select(distinct_distance_data, id, delivery_store, store_list),
                  by = "id") %>% 
        select(-id)
    
    store_transpot_data <- deliveries_summary %>% 
        left_join(store_distance,
                  by = c("delivery_store", "recipient_store", "store_list"),
                  multiple = "any") %>% 
        mutate(date = floor_date(date_delivery - years(2000), unit = "month"),
               distance = ifelse(route_parent %in% "000000002", # Хмельницький
                                 distance/2,
                                 distance)) %>% 
        group_by(date, vehicle_id, recipient_store) %>% 
        summarise(store_distance = sum(distance)) %>%
        ungroup() %>% 
        left_join(truck_1km_cost, by = c("date", "vehicle_id")) %>% 
        mutate(store_vehicle_cost = store_distance * cost_1km) %>% 
        group_by(date, recipient_store) %>% 
        summarise(total_cost = sum(store_vehicle_cost)) %>% 
        group_by(recipient_store) %>% 
        summarise(avr_transport_cost = mean(total_cost, na.rm = TRUE)) %>% 
        left_join(select(ref_store, store_id, subdiv_id),
                  by = c("recipient_store" = "store_id")) %>%
        add_row(subdiv_id = "000000246", avr_transport_cost = 0L) %>%  # Луцьк Ковельськ
        left_join(lutsk_stock_motion_share, by = "subdiv_id") %>%
        mutate(avr_transport_cost = ifelse(is.na(lutsk_subdiv_share),
                                           round(avr_transport_cost),
                                           round(avr_transport_cost[
                                               subdiv_id == "000000197"] *
                                                   lutsk_subdiv_share +
                                                   avr_transport_cost))) %>% 
        select(-recipient_store, -lutsk_subdiv_share) %>% 
        filter(subdiv_id != "000000197")
    
}


get_retail_subdiv_areas <- function() {
    
    subdiv_areas <- ref_shop_area %>% 
        group_by(subdiv_id) %>% 
        filter(date == max(date)) %>% 
        select(-date) %>% 
        pivot_wider(names_from = type_area, values_from = area_value) %>% 
        mutate(across(where(is.numeric), ~replace_na(.x, 0)),
               effective_area = total_area - b2b_area) %>% 
        select(subdiv_id, total_area, trade_area, effective_area)
        
}


get_sale_retail_raw <- function(date_finish, trade_subdiv) {
    
    get_sale_retail_common(date_finish - months(12) + years(2000),
                               date_finish + years(2000)) %>%
        filter(subdiv_id %in% trade_subdiv) %>%
        mutate(date = floor_date(date - years(2000), unit = "month"),
               subdiv_id = ifelse(subdiv_id %in% "000000189",   # Копіцентр ХМ СМ
                                  "000000007",
                                  subdiv_id))
}


get_vat_tax <- function(date_finish, sale_retail_raw) {
    
    sale_korvet_service <- get_sale_service_data(date_finish - months(3) +
                                                     years(2000),
                                                 date_finish + years(2000)) %>% 
        filter(organization_id %in% "000000001") %>% 
        mutate(date = floor_date(sale_doc_date, unit = "month"),
               subdiv_id = ifelse(subdiv_id %in% "000000236",  # PROM
                                  "000000168",                 # Интернет-магазин
                                  subdiv_id)) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(korvet_service_gross_profit = sum(gross_profit))
    
    vat_data <- tibble(date = seq.Date(date_finish - months(3), date_finish-months(1),
                                       by = "month")) %>% 
        mutate(account = "6412",
               vat_sum = map2_dbl(date + years(2000), account, account_balance),
               vat_sum = ifelse(vat_sum < 0,
                                -round(vat_sum),
                                0)) %>% 
        select(date, vat_sum)
    
    retail_vat_data <- sale_retail_raw %>% 
        filter(date >= date_finish - months(3),
               organization_id %in% "000000001") %>% 
        mutate(gross_profit = checks_report_sum - doc_cost_sum) %>% 
        group_by(date, subdiv_id) %>% 
        summarise(korvet_retail_gross_profit = sum(gross_profit)) %>% 
        
        bind_rows(sale_korvet_service) %>% 
        left_join(vat_data, by = "date") %>% 
        group_by(date) %>% 
        mutate(total_gross_profit = sum(korvet_retail_gross_profit, na.rm = TRUE) +
                   sum(korvet_service_gross_profit, na.rm = TRUE),
               gross_profit_share = ifelse(
                   !is.na(korvet_retail_gross_profit),
                   korvet_retail_gross_profit / total_gross_profit,
                   korvet_service_gross_profit / total_gross_profit),
               subdiv_vat = gross_profit_share * vat_sum
        ) %>% 
        filter(!is.na(korvet_retail_gross_profit)) %>% 
        group_by(subdiv_id) %>% 
        summarise(avr_vat_sum = round(mean(subdiv_vat, na.rm = TRUE)))
    
}


remove_unnecessary_items <- function(data) {
    
    data <- data %>% left_join(select(ref_items, item_id, main_parent),
                                   by = "item_id") %>% 
        filter(!main_parent %in% c("000500558",           # Бухгалтерские
                                   "000300000",           # Торгів.обладнання
                                   "000400010")           # Рекламні материали
        ) %>% 
        select(-main_parent)

}


get_date_inventory <- function(date_finish, inventory_months = 3){
    
    calculate_date_inventory <- function(date_start) {
        
        inventory_data <- inventory_movement_data %>% 
            filter(date_movement >= date_start) %>% 
            group_by(store_id, item_id) %>% 
            summarise(store_item_moving_qty = sum(store_item_moving_qty))%>% 
            ungroup() %>% 
            full_join(current_inventory, by = c("store_id", "item_id")) %>% 
            replace_na(list(item_store_qty = 0,
                            store_item_moving_qty = 0)
            ) %>%
            mutate(balance_qty = item_store_qty - store_item_moving_qty
            ) %>% 
            filter(balance_qty != 0) %>% 
            select(-store_item_moving_qty, -item_store_qty)
        
    }
    
    
    calculate_date_inventory_batch <- function(date_start) {
        
        inventory_data_2 <- inventory_movement_batch_data %>% 
            filter(date_movement >= date_start) %>% 
            select(-date_movement) %>% 
            rename(item_store_qty = item_motion_qty,
                   item_store_sum = item_motion_cost) %>% 
            mutate(item_store_qty = -item_store_qty,
                   item_store_sum = -item_store_sum) %>% 
            bind_rows(current_inventory_batch) %>% 
            group_by(store_id, item_id, date_receipt) %>%
            summarise(balance_batch_qty = sum(item_store_qty),
                      balance_batch_sum = sum(item_store_sum))%>%
            ungroup() %>%
            filter(!near(balance_batch_qty, 0),
                   !(balance_batch_qty < 0 & balance_batch_sum > 0))
    }
    
    calculate_inventory_sum <- function(date_start, data, data_batch) {
        
        joined_data <- left_join(data, data_batch,
                                 by = c("store_id", "item_id"),
                                 multiple = "all") %>%
            arrange(store_id, item_id, desc(date_receipt)) %>% 
            group_by(store_id, item_id) %>% 
            mutate(cum_balance_batch_qty = cumsum(balance_batch_qty),
                   cum_balance_batch_sum = cumsum(balance_batch_sum))
        
        equal_data <- joined_data %>% 
            filter(max(cum_balance_batch_qty) == balance_qty) %>% 
            filter(cum_balance_batch_qty == balance_qty) %>%
            slice(1) %>% 
            transmute(balance_sum = cum_balance_batch_sum) %>% 
            ungroup()
        
        batch_more_data <- joined_data %>% 
            filter(cum_balance_batch_qty > balance_qty) %>% 
            slice(1) %>% 
            transmute(balance_sum = ifelse(
                balance_batch_qty == cum_balance_batch_qty,
                round(balance_batch_sum / balance_batch_qty * balance_qty, 2),
                round(cum_balance_batch_sum - balance_batch_sum +
                          balance_batch_sum / balance_batch_qty *
                          (balance_batch_qty - (cum_balance_batch_qty - balance_qty)
                          ),
                      2))) %>% 
            ungroup()
        
        batch_less_data <- joined_data %>% 
            filter(max(cum_balance_batch_qty) < balance_qty) %>% 
            slice_tail(n = 1) %>% 
            transmute(balance_sum = ifelse(
                balance_batch_qty == cum_balance_batch_qty,
                round(balance_batch_sum / balance_batch_qty * balance_qty, 2),
                round(cum_balance_batch_sum +
                          balance_batch_sum / balance_batch_qty *
                          (balance_qty - cum_balance_batch_qty),
                      2))) %>% 
            ungroup()
        
        without_batch <- joined_data %>% 
            filter(is.na(cum_balance_batch_qty)) %>% 
            select(store_id:balance_qty) %>%  
            ungroup() %>% 
            mutate(date = date_start) %>% 
            left_join(prices_df,
                      join_by(item_id, closest(date > date_setting))) %>% 
            mutate(balance_sum = balance_qty * item_price) %>% 
            select(store_id, item_id, balance_sum)
        
        result <- bind_rows(equal_data, batch_more_data, batch_less_data,
                            without_batch) %>% 
            left_join(select(ref_items, item_id, group_id),
                      by = "item_id") %>% 
            left_join(select(ref_item_group, group_id, category_id),
                      by = "group_id") %>% 
            filter(!is.na(category_id)) %>% 
            group_by(store_id, category_id) %>% 
            summarise(category_inventory_sum = sum(balance_sum, na.rm = TRUE),
                      category_sku = sum(balance_sum > 0))
        
    }
    
    
    current_inventory <- get_current_inventory() %>% 
        remove_unnecessary_items()
    
    inventory_movement_data <- get_inventory_movement(
        date_finish - months(inventory_months)) %>% 
        remove_unnecessary_items()
    
    current_inventory_batch <- get_current_inventory_batch() %>% 
        remove_unnecessary_items()
    
    inventory_movement_batch_data <- get_inventory_movement_batch(
        date_finish - months(inventory_months)) %>% 
        remove_unnecessary_items()
    
    prices_df <- price_items_data(date_finish - months(inventory_months)) %>% 
        mutate(date_setting = as_date(date_setting) - years(2000)) %>% 
        group_by(item_id, date_setting) %>% 
        slice_tail(n=1) %>% 
        ungroup()

    
    inventory_data <- tibble(
        date = seq.Date(date_finish - months(inventory_months),
                        date_finish,
                        by = "month")
    ) %>%
        mutate(data = map(date, calculate_date_inventory)) %>% 
        mutate(data_batch = map(date, calculate_date_inventory_batch)) %>% 
        mutate(data_sum = pmap(list(date, data, data_batch),
                           calculate_inventory_sum)) %>% 
        select(date, data_sum)

}


get_discounts_data <- function(discounts_raw) {
    
    discounts_data <- discounts_raw %>% 
        filter(increment != 0 | account_increment != 0,
               name != "Округление на вид оплаты") %>% 
        mutate(name_marketing = case_when(
                   name %in% "5% на бонусний рахунок"  ~ "Нарахування_бонусів",
                   name %in% "Використання бонусів"  ~ "Використання_бонусів",
                   .default = "Знижка_Маркетинг"
               )) %>% 
        group_by(code_subdivision, name_marketing) %>% 
        summarise(total_discount_sum = round(-sum(increment) +
                                                 sum(account_increment))) %>%
        ungroup() %>% 
        pivot_wider(names_from = name_marketing,
                    values_from = total_discount_sum) %>% 
        left_join(select(ref_store, store_id, subdiv_id),
                         by = c("code_subdivision" = "store_id")) %>% 
        select(-code_subdivision)
    
}


get_sale_structure <- function(date_finish) {
    
    # category_cost_data <- get_sale_retail_category_cost(date_finish - months(3)+
    #                                                         years(2000),
    #                                                     date_finish +
    #                                                         years(2000)) %>% 
    #     left_join(ref_global_category, by = "category_id") %>% 
    #     group_by(subdiv_id, global_category) %>% 
    #     summarise(global_category_cost_sum = sum(category_cost_sum)) %>% 
    #     ungroup()
    
    sale_retail_detailed <- get_sale_retail_item_category(date_finish -
                                                                  months(3) +
                                                                  years(2000),
                                                              date_finish +
                                                                  years(2000)
                                                              )
   
    subdiv_total_sum <- sale_retail_detailed %>% 
        group_by(subdiv_id) %>% 
        summarise(subdiv_sale_sum = sum(check_category_sum))
    
    # subdiv_total_profit <- category_cost_data %>% 
    #     group_by(subdiv_id) %>% 
    #     summarise(subdiv_cost_sum = sum(global_category_cost_sum)) %>% 
    #     left_join(subdiv_total_sum, by = "subdiv_id") %>% 
    #     mutate(subdiv_total_profit = subdiv_sale_sum - subdiv_cost_sum)
    
    subdiv_total_checks <- sale_retail_detailed %>% 
        group_by(subdiv_id) %>% 
        summarise(subdiv_total_check_qty = n_distinct(nmbr_checks_report,
                                                      check_nmbr))
    
    
    sale_structure <- sale_retail_detailed %>% 
        # filter(item_category_qty > 0) %>% 
        group_by(subdiv_id, category_id) %>% 
        summarise(category_total_checks = n(),
                  category_total_sum = sum(check_category_sum)) %>% 
        ungroup() %>% 
        # left_join(category_cost_data, by = c("subdiv_id", "global_category")) %>%
        # left_join(subdiv_total_profit, by = "subdiv_id") %>% 
        left_join(subdiv_total_sum, by = "subdiv_id") %>% 
        left_join(subdiv_total_checks, by = "subdiv_id") %>% 
        mutate(category_checks_share = round(category_total_checks /
                                                 subdiv_total_check_qty,
                                             3),
               category_revenue_share = round(category_total_sum /
                                                  subdiv_sale_sum,
                                              3)) %>% 
        select(subdiv_id, category_id, category_checks_share,
               category_revenue_share) %>% 
        filter(!category_id %in% "000000013")
        # pivot_longer(cols = category_checks_share:category_profit_share,
        #              names_to = "index") %>% 
        # group_by(subdiv_id, index) %>% 
        # filter(value == max(value)) %>% 
        # ungroup() %>% 
        # pivot_wider(names_from = index,
        #             values_from = "value") %>% 
        # mutate(flow_generator = paste0(global_category, "(",
        #                                category_checks_share * 100, "%)"),
        #        profit_generator = paste0(global_category, "(",
        #                                  category_profit_share * 100, "%)")) %>% 
        # select(subdiv_id, flow_generator, profit_generator)
    
}


get_delivery_raw <- function(date_finish) {
    
    delivery_raw <- get_delivery_data(date_finish - months(3) + years(2000),
                                      date_finish + years(2000)) %>% 
        filter(!is.na(vehicle_name),
               speedo_finish > 0,
               is_not_delivered == FALSE) %>% 
        mutate(time_return = case_when(
            year(time_return) > 2021 & year(time_return) < year(time_depart) ~ 
                time_return +
                years(year(time_depart) -
                          year(time_return)),
            day(time_return) < day(time_depart) ~ time_return + 
                days(day(time_depart) -
                         day(time_return)),
            .default = time_return)) %>%
        mutate(distance = speedo_finish - speedo_start,
               date = floor_date(date_delivery - years(2000), unit = "month"),
               duration = round(as.numeric((time_return - time_depart)/60), 1))
    
}


make_calculation <- function(date_finish) {
    
    trade_subdiv <- ref_subdiv_category %>%
        pull(subdiv_id)
    
    trade_subdiv_stores <- ref_subdiv_category %>% 
        filter(!subdiv_category %in% " Супермаркети  В2В") %>% 
        select(subdiv_id) %>% 
        left_join(select(ref_subdiv, subdiv_id, store_id),
                  by = "subdiv_id") %>%
        pull(store_id)
    
    lutsk_distribution_store <- ref_store %>% 
        filter(distribution_store == "000001073",
               marked == FALSE,
               store_id != "000001073") %>% 
        select(store_id, store_name)
    
    sale_retail_raw <- get_sale_retail_raw(date_finish,
                                           trade_subdiv)
    
    expenses_data <- get_expenses_data(date_finish)
    
    salary_data <- get_salary_data(date_finish)
    
    direct_costs <- get_direct_costs(expenses_data, salary_data,
                                     date_finish, trade_subdiv)
    
    shipment_share <- get_shipment_share(date_finish, trade_subdiv_stores)
    
    super_shipment_share <- shipment_share %>% 
        left_join(ref_subdiv_category) %>% 
        filter(subdiv_category == " Супермаркети  В2С",
               type_subdiv == "retail") %>% 
        select(subdiv_id, avr_subdiv_share)
    
    distribution_stores_motion_data <- get_stock_motion(c("000000001", "000001073"),
                                                        date_finish - months(3) + years(2000),
                                                        date_finish + years(2000)) %>% 
        filter(type_motion == 1,
               !(is.na(nmbr_moving_out) & is.na(nmbr_sale_doc)))
    
    lutsk_stock_motion_share <- get_lutsk_stock_motion_share(distribution_stores_motion_data,
                                                             lutsk_distribution_store)
    
    main_stock_motion_share <- get_main_stock_motion_share(distribution_stores_motion_data,
                                                           lutsk_stock_motion_share,
                                                           super_shipment_share,
                                                           trade_subdiv)
    
    distribution_stores_cost <- get_distribution_stores_cost(direct_costs,
                                                             salary_data,
                                                             main_stock_motion_share,
                                                             lutsk_stock_motion_share)
    
    delivery_raw <- get_delivery_raw(date_finish)
    
    truck_1km_cost <- get_truck_1km_cost(delivery_raw,
                                         salary_data,
                                         expenses_data,
                                         date_finish)
    
    transport_costs <- get_subdiv_transport_cost(delivery_raw,
                                                 truck_1km_cost,
                                                 lutsk_stock_motion_share,
                                                 date_finish,
                                                 trade_subdiv_stores,
                                                 lutsk_distribution_store)
    
    subdiv_areas <- get_retail_subdiv_areas()
    
    inventory_data <- get_date_inventory(date_finish)
    
    inventory_df <- inventory_data %>% 
        unnest(cols = c(data_sum)) %>% 
        group_by(store_id, date) %>% 
        summarise(month_inventory = sum(category_inventory_sum),
                  month_sku_qty = sum(category_sku, na.rm = TRUE)) %>% 
        ungroup() %>% 
        filter(store_id %in% trade_subdiv_stores) %>% 
        left_join(select(ref_store, store_id, subdiv_id),
                  by = "store_id") %>% 
        group_by(subdiv_id) %>% 
        summarise(avr_inventory = round(mean(month_inventory)),
                  avr_sku_qty   = round(mean(month_sku_qty)))
    
    category_inventory <- inventory_data %>% 
        unnest(cols = c(data_sum)) %>% 
        filter(store_id %in% trade_subdiv_stores) %>% 
        group_by(store_id, category_id, date) %>% 
        summarise(category_month_inventory = sum(category_inventory_sum)) %>% 
        ungroup() %>%
        group_by(store_id, category_id) %>% 
        summarise(avr_category_inventory = mean(category_month_inventory)) %>% 
        ungroup() %>% 
        left_join(select(ref_store, store_id, subdiv_id),
                  by = "store_id") %>%
        select(-store_id)
    
    vat_data <- get_vat_tax(date_finish, sale_retail_raw)
    
    discounts_raw <- get_retail_discounts(date_finish - months(3),
                                          date_finish)
    
    discounts_data <- get_discounts_data(discounts_raw)
    
    sale_structure <- get_sale_structure(date_finish)
    
    subdiv_category_structure <- category_inventory %>%
        group_by(subdiv_id) %>% 
        mutate(category_inventory_share = round(avr_category_inventory / 
                                                    sum(avr_category_inventory),
                                                3)) %>%
        ungroup() %>% 
        select(-avr_category_inventory) %>% 
        filter(!category_id %in% "000000013") %>% 
        full_join(sale_structure, by = c("subdiv_id", "category_id")) %>% 
        filter(!is.na(category_revenue_share)) %>% 
        left_join(select(ref_item_category, category_id, category_name),
                  by = "category_id") %>% 
        select(-category_id)
    
    max_category_revenue_share <- subdiv_category_structure %>% 
        group_by(subdiv_id) %>% 
        slice_max(order_by = category_revenue_share, with_ties = FALSE) %>% 
        mutate(revenue_generator = paste0(category_name, " - ",
                                          category_revenue_share *100, "%")) %>% 
        select(subdiv_id, revenue_generator)
    
    sale_retail_data <- sale_retail_raw %>% 
        group_by(date, subdiv_id) %>% 
        summarise(revenue = sum(checks_report_sum),
                  COG = sum(doc_cost_sum),
                  month_checks_qty = sum(total_checks_qty),
                  month_sku_in_checks = sum(checks_report_sku),
                  month_units_qty = sum(checks_report_qty),
                  month_card_owners_checks_qty = sum(doc_reg_cust_nmbr,
                                                     na.rm = TRUE)) %>% 
        ungroup()
    
    category_df <- sale_retail_data %>% 
        distinct(subdiv_id) %>% 
        left_join(ref_shop_area %>% 
                      filter(type_area == "trade_area") %>% 
                      select(subdiv_id, area_value),
                  by = "subdiv_id") %>% 
        left_join(ref_subdiv_category, by = "subdiv_id") %>% 
        mutate(trade_category = case_when(
            area_value > 100 & subdiv_category %in% " Супермаркети  В2С" ~ "Супер",
            area_value >= 100                                            ~ "Максі",
            area_value >   60                                            ~ "Міді",
            area_value <=  60                                            ~ "Міні"),
        ) %>% 
        mutate(trade_category = fct_relevel(trade_category, c("Міні",
                                                              "Міді",
                                                              "Максі",
                                                              "Супер")))
    
    
    result_subdiv <- sale_retail_data %>% 
        filter(date >= date_finish - months(3)) %>% 
        group_by(subdiv_id) %>% 
        summarise(total_revenue = sum(revenue),
                  avr_revenue = round(mean(revenue)),
                  total_COG = sum(COG),
                  avr_COG = round(mean(COG)),
                  total_checks_qty = sum(month_checks_qty),
                  avr_checks_qty = round(mean(month_checks_qty)),
                  total_sku_in_checks = sum(month_sku_in_checks),
                  total_units_qty = sum(month_units_qty),
                  total_card_owners_checks_qty = sum(month_card_owners_checks_qty)
        ) %>% 
        left_join(subdiv_areas, by = "subdiv_id") %>% 
        left_join(inventory_df, by = "subdiv_id") %>% 
        left_join(direct_costs, by = "subdiv_id") %>% 
        left_join(shipment_share %>% 
                      filter(type_subdiv %in% "retail") %>% 
                      select(-type_subdiv),
                  by = "subdiv_id") %>%
        left_join(distribution_stores_cost, by = "subdiv_id") %>% 
        left_join(transport_costs, by = "subdiv_id") %>% 
        left_join(vat_data, by = "subdiv_id") %>% 
        left_join(discounts_data, by = "subdiv_id") %>%
        left_join(max_category_revenue_share, by = "subdiv_id") %>%
        left_join(ref_subdiv_category, by = "subdiv_id") %>% 
        mutate(across(where(is.numeric), ~replace_na(.x, 0))) %>% 
        mutate(avr_gross_profit = avr_revenue - avr_COG,
               avr_check_sum = round(total_revenue / total_checks_qty, 1),
               avr_ckecks_sku_qty = round(total_sku_in_checks / total_checks_qty, 2),
               card_owners_share = round(total_card_owners_checks_qty /
                                             total_checks_qty, 2),
               avr_check_units = round(total_units_qty / total_checks_qty, 2),
               avr_unit_price = round(total_revenue / total_units_qty, 2),
               revenue_per_1m = round(avr_revenue / trade_area * avr_subdiv_share),
               content_1m = round((avr_rent_sum + utilities_exp_sum) / total_area),
               trade_area_share = round(trade_area / total_area, 2),
               gmros = round(avr_gross_profit / trade_area),
               employee_workload = round(avr_checks_qty / stuff_units),
               gmrol = round(avr_gross_profit / avr_working_hours),
               gmroi = round(avr_gross_profit / avr_inventory * avr_subdiv_share,2),
               avr_inventory_1m = round(avr_inventory / effective_area),
               sku_per_1m = round(avr_sku_qty / effective_area),
               logistic_share = round((distr_stores_cost + avr_transport_cost * 
                                           avr_subdiv_share) /
                                          avr_COG, 2),
               area_maintenance = ifelse(
                   subdiv_category %in% " Супермаркети  В2С",
                   round(avr_rent_sum + utilities_exp_sum),
                   round((avr_rent_sum + utilities_exp_sum) * avr_subdiv_share)),
               total_expenses = ifelse(
                   subdiv_category %in% " Супермаркети  В2С",
                   round((avr_total_exp_sum + avr_salary_sum + area_maintenance + 
                             distr_stores_cost + avr_transport_cost + avr_vat_sum) +
                             avr_revenue * 0.05),
                   round((avr_total_exp_sum + avr_salary_sum + avr_vat_sum + 
                             area_maintenance + (distr_stores_cost + avr_transport_cost) *
                             avr_subdiv_share) + avr_revenue * 0.05)),
               net_profit = avr_gross_profit - total_expenses,
               profitibility = round(net_profit / avr_revenue, 2),
               discounts_share = round(`Знижка_Маркетинг` / total_revenue, 3),
               paid_bonus_share = round(`Використання_бонусів` / total_revenue, 3),
               discounts_bonuses_share = round((`Знижка_Маркетинг` +
                                                    `Використання_бонусів`) /
                                                   total_revenue, 3),
               margin = round(avr_gross_profit / avr_revenue, 3),
               other_expenses = total_expenses - area_maintenance - avr_salary_sum
        ) %>% 
        left_join(select(category_df, subdiv_id, trade_category),
                  by = "subdiv_id") %>% 
        left_join(select(ref_subdiv, subdiv_id, subdiv_name),
                  by = "subdiv_id") %>% 
        mutate(subdiv_name = str_replace(subdiv_name, "ТоргівельнийЦентр", "ТЦ"),
               subdiv_name = str_replace(subdiv_name, "[Р|р]оздріб", ""),
               subdiv_name = str_replace(subdiv_name, "Кам'янець-Подільський", "Кам-Под") 
        )
    
    new_card_data <- get_new_cards(date_finish - months(3),
                                   date_finish) %>% 
        filter(store %in% trade_subdiv_stores) %>% 
        left_join(select(ref_store, store_id, subdiv_id),
                  by = c("store" = "store_id"))

    card_owner_purchases <- discounts_raw %>% 
        filter(!is.na(client)) %>% 
        mutate(date = as_date(date)) %>% 
        group_by(code_subdivision, client) %>% 
        summarise(total_purchase = n_distinct(date))
    
    subdiv_cards_data <- card_owner_purchases %>% 
        filter(code_subdivision %in% trade_subdiv_stores) %>% 
        left_join(select(ref_store, store_id, subdiv_id),
                  by = c("code_subdivision" = "store_id")) %>% 
        group_by(subdiv_id) %>% 
        summarise(purch_1 = sum(total_purchase == 1),
                  total_purch = n()) %>% 
        mutate(purch_more_1_share = round(1 - purch_1 / total_purch, 2)) %>% 
        left_join(select(new_card_data, subdiv_id, new_cards = count)) %>% 
        left_join(select(result_subdiv, subdiv_id, card_owners_share)) %>% 
        left_join(select(ref_subdiv, subdiv_id, subdiv_name)) %>%
        left_join(category_df, by = "subdiv_id") %>% 
        select(subdiv_name, trade_category, card_owners_share, total_purch, 
               purch_more_1_share, new_cards) %>% 
        replace_na(list(new_cards = 0))
    
    quarter_data <- sale_retail_raw %>% 
        mutate(quarter = case_when(
            date < (date_finish - months(9)) ~ 1,
            date < (date_finish - months(6)) ~ 2,
            date < (date_finish - months(3)) ~ 3,
            .default = 4
        ),
        quarter = as.factor(quarter)
        ) %>% 
        left_join(select(category_df, subdiv_id, trade_category),
                  by = "subdiv_id") %>% 
        group_by(trade_category, quarter) %>% 
        summarise(total_revenue = sum(checks_report_sum),
                  total_checks_qty = sum(total_checks_qty),
                  total_sku_in_checks = sum(checks_report_sku),
                  total_units_qty = sum(checks_report_qty)) %>% 
        ungroup() %>% 
        mutate(avr_check_sum = round(total_revenue / total_checks_qty, 1),
               avr_ckecks_sku_qty = round(total_sku_in_checks / total_checks_qty, 2),
               avr_check_units = round(total_units_qty / total_checks_qty, 2),
               avr_unit_price = round(total_revenue / total_units_qty, 2)) %>% 
        #filter(quarter %in% c(1,2)) %>% 
        group_by(trade_category) %>% 
        summarise(check_sku_qty_growth = round(
            ifelse(avr_ckecks_sku_qty[quarter == 2] > avr_ckecks_sku_qty[quarter == 1],
                   1 + (avr_ckecks_sku_qty[quarter == 2] /
                            avr_ckecks_sku_qty[quarter == 1] - 1) / 2,
                   1 - (1 - avr_ckecks_sku_qty[quarter == 2] /
                            avr_ckecks_sku_qty[quarter == 1]) / 2),
            2),
            check_units_growth = round(
                ifelse(avr_check_units[quarter == 2] > avr_check_units[quarter == 1],
                       1 + (avr_check_units[quarter == 2] /
                                avr_check_units[quarter == 1] - 1) / 2,
                       1 - (1 - avr_check_units[quarter == 2] /
                                avr_check_units[quarter == 1]) / 2),
                2)
        )
    
    return(list(result_subdiv, subdiv_category_structure, category_df,
                subdiv_cards_data, quarter_data))
    
}


mode <- function(vec) {
    extr <- sub("^(.*) - .*", "\\1", vec)
    freq <- table(extr)
    names(freq)[which.max(freq)]
}