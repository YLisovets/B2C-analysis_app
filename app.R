library(shiny)
library(shinydashboard)
library(shinyalert)
library(shinybusy)

library(tidyverse)
library(lubridate)

library(DBI)
library(odbc)
library(RMySQL)
library(dbplyr)

library(reactablefmtr)
library(DiagrammeR)


source("script/functions.R")
source("script/collect_data.R")


ui <- dashboardPage(skin = "red",
    dashboardHeader(title = "Аналіз магазинів"),
    dashboardSidebar(
        sidebarMenu(
            menuItem("Показники категорій", tabName = "all_categories",
                     icon = icon("table-columns")),
            menuItem("Показники підрозділів", tabName = "selected_category",
                     icon = icon("table-columns")),
            menuItem("Структура підрозділів", tabName = "structure_category",
                     icon = icon("table-columns")),
            menuItem("Власники карток", tabName = "card_owners",
                     icon = icon("table-columns")),
            menuItem("Аналіз підрозділу", tabName = "selected_subdiv",
                     icon = icon("bullseye")),
            hr(),
            actionButton(
                inputId = "apply",
                label = "Розрахувати",
                icon = icon("play"),
                class = "btn-primary"
            ),
            hr(),
            
            tags$div(
                h5(HTML("Додаток розраховує середні показники за 3 місяці в залежності від поточної дати.
Якщо додаток запускається після 20 числа, то останній місяць - минулий місяць, до 20 - місяць, що передує минулому.")), 
                style="white-space:break-spaces; display: flex; padding-left: 12px;"
                )
            )
    ),
    dashboardBody(
        tabItems(
            # First tab content
            tabItem(tabName = "all_categories",
                    fluidRow(
                        box(reactableOutput("category_table"))
                    )
            ),
            
            # Second tab content
            tabItem(tabName = "selected_category",
                    fluidRow(
                        box(selectInput(
                            inputId = "trade_category_selected",
                            label   =  "Оберіть категорію магазинів:",
                            choices = c("Міні", "Міді", "Максі", "Супер"),
                            selected = "Супер"
                            ),
                            width = 5),
                        
                        box(reactableOutput("subdiv_table"),
                            width = 12)
                    )
            ),
            
            # Third tab content
            tabItem(tabName = "structure_category",
                    fluidRow(
                        box(reactableOutput("cheks_share_table"),
                            title = "Частка категорій у чеках",
                            width = 12),
                        
                        box(reactableOutput("revenue_share_table"),
                            title = "Частка категорій у виручці(залишках)",
                            width = 12)
                    )
            ),
            
            # Fourth tab content
            tabItem(tabName = "card_owners",
                    fluidRow(
                        box(reactableOutput("card_owners_table"),
                            width = 12)
                    )
            ),
            
            # Fifth tab content
            tabItem(tabName = "selected_subdiv",
                    fluidRow(
                        box(selectInput(
                            inputId = "subdiv_selected",
                            label   =  "Оберіть підрозділ:",
                            choices = ""
                            ),
                            width = NULL),

                        box(grVizOutput("diagram"),
                             width = NULL),
                        
                        box(DT::dataTableOutput("proposal_table"),
                             width = NULL)
                        )
                    )
            )
        )
)


server <- function(input, output, session) {
    
    observeEvent(eventExpr = input$trade_category_selected, handlerExpr = {
        
        req(rv$result_tbl)
        
        result_selected <- reactive({
            filter(rv$result_tbl, trade_category == input$trade_category_selected)
        })
        
        updateSelectInput(
            session = session,
            inputId = "subdiv_selected",
            choices = result_selected()$subdiv_name
        )
        
    })
    
    observeEvent(eventExpr = rv$result_tbl, handlerExpr = {
        
        req(input$trade_category_selected)
        
        updateSelectInput(
            session = session,
            inputId = "subdiv_selected",
            choices = rv$result_tbl$subdiv_name[
                rv$result_tbl$trade_category == input$trade_category_selected]
        )
        
    })

    
    # Setup Reactive Values ----
    
    rv <- reactiveValues()
    
    observeEvent(input$apply, {
        
        show_modal_spinner(
            spin = "cube-grid",
            color = "firebrick",
            text = "Триває розрахунок..."
        )
        
        
        # загрузка даних та попередній розрахунок
        

        date_finish <- if_else(day(Sys.Date()) >= 20,
                               floor_date(Sys.Date(), unit = "month"),
                               floor_date(Sys.Date(), unit = "month") - months(1))
        
        con_db <<- connect_to_db()
        
        con_ukm <<- connect_to_ukm()
        
        get_references()
        
        rv$result <- make_calculation(date_finish)   #read_rds("result.rds")
        
        dbDisconnect(con_db)
        
        dbDisconnect(con_ukm)
        
        rv$result_tbl <- rv$result[[1]]
        
        rv$subdiv_category_structure <- rv$result[[2]]
        
        rv$subdiv_trade_category <- rv$result[[3]] %>% 
            select(subdiv_id, trade_category)
        
        rv$subdiv_cards_data <- rv$result[[4]]
        
        rv$quarter_data <- rv$result[[5]]
        
        rv$quantile_75 <- rv$result_tbl %>% 
            select(trade_category,
                   profitibility, net_profit, avr_gross_profit,
                   avr_salary_sum, stuff_units, employee_workload, gmrol,
                   area_maintenance, total_area, content_1m, avr_subdiv_share, 
                   total_expenses, other_expenses, logistic_share,
                   margin, discounts_share, paid_bonus_share, revenue_generator,
                   avr_revenue, revenue_per_1m, gmros, trade_area_share,
                   avr_checks_qty, avr_check_sum, avr_check_units, avr_unit_price,
                   avr_ckecks_sku_qty,card_owners_share,
                   avr_inventory_1m, gmroi
            ) %>% 
            group_by(trade_category) %>% 
            summarise(across(where(is.numeric), ~quantile(.x, probs = 0.75)),
                      revenue_generator = mode(revenue_generator)) %>%
            mutate(across(where(is.numeric), unname)) %>% 
            mutate(subdiv_name = "quantile_75")
        
        rv$quantile_25 <- rv$result_tbl %>% 
            select(trade_category,
                   profitibility, net_profit, avr_gross_profit,
                   avr_salary_sum, stuff_units, employee_workload, gmrol,
                   area_maintenance, total_area, content_1m, avr_subdiv_share,
                   total_expenses, other_expenses, logistic_share,
                   margin, discounts_share, paid_bonus_share, revenue_generator,
                   avr_revenue, revenue_per_1m, gmros, trade_area_share,
                   avr_checks_qty, avr_check_sum, avr_check_units, avr_unit_price,
                   avr_ckecks_sku_qty,card_owners_share,
                   avr_inventory_1m, gmroi
            ) %>% 
            group_by(trade_category) %>% 
            summarise(across(where(is.numeric), ~quantile(.x, probs = 0.25)),
                      revenue_generator = mode(revenue_generator)) %>%
            mutate(across(where(is.numeric), unname)) %>% 
            mutate(subdiv_name = "quantile_25")

        remove_modal_spinner()

    }, once = TRUE)
    
    
   result_category <- reactive({
        
        req(rv$result_tbl)
        
        rv$result_tbl %>% 
            group_by(trade_category) %>% 
            summarise("Кількість магазинів" = n(),
                      "Загальна торгова площа" = round(sum(trade_area)),
                      GMROS = round(sum(avr_gross_profit) / sum(trade_area)),
                      GMROL = round(sum(avr_gross_profit) / sum(avr_working_hours)),
                      GMROI = round(sum(avr_gross_profit) / sum(avr_inventory *
                                                                avr_subdiv_share),
                                    2),
                      "Залишки на 1м" = round(mean(avr_inventory_1m)),
                      "SKU на 1м" = round(sum(avr_sku_qty) / sum(effective_area)),
                      "Навантаження на працівника" = round(mean(employee_workload)),
                      "Середня сума чеку" = round(sum(total_revenue) /
                                                      sum(total_checks_qty),
                                                  1),
                      "Од-ць у чеку" = round(sum(total_units_qty) /
                                                 sum(total_checks_qty),
                                             2),
                      "Ціна од-ці" = round(sum(total_revenue) / sum(total_units_qty),
                                           2),
                      "Кіл-ть SKU у чеку" = round(sum(total_sku_in_checks) /
                                                      sum(total_checks_qty),
                                                  2),
                      "Рентабельність прибутку" = round(sum(net_profit) / 
                                                            sum(avr_revenue),
                                                        2)) %>% 
            pivot_longer(-trade_category, names_to = "index",
                         values_to = "Значення") %>%
            pivot_wider(names_from = "trade_category", values_from = "Значення")
    })

    output$category_table <- renderReactable({
        
        req(result_category())
        
        result_category() %>% 
            reactable(
                striped = TRUE,
                bordered = TRUE,
                pagination = FALSE,
                highlight = TRUE,
                fullWidth = FALSE,
                theme = reactableTheme(headerStyle = list(backgroundColor = "#EED8AE",
                                                          fontSize = "18px"),
                                       cellStyle = list(fontFamily = "Monaco, monospace", 
                                                        fontSize = "16px")
                ),
                columns = list(
                    index = colDef(name = "Показник / Категорія",
                                   minWidth = 250)
                )
            )
    })

    
    category_subdiv_tbl <- reactive({

        req(input$trade_category_selected, rv$result_tbl)

        rv$result_tbl %>%
            filter(trade_category %in% input$trade_category_selected) %>%
            select(subdiv_name,
                   "Загальна площа" = total_area,
                   "Утримання 1м2" = content_1m,
                   "Частка торг.площі" = trade_area_share,
                   "Виручка з 1м2" = revenue_per_1m,
                   GMROS = gmros,
                   "Кіл-ть персоналу" = stuff_units,
                   "Завант.персоналу" = employee_workload,
                   GMROL = gmrol,
                   "Сума залишків" = avr_inventory,
                   "Залишки на 1м2" = avr_inventory_1m,
                   CMROI = gmroi,
                   "Кіл-ть SKU" = avr_sku_qty,
                   "SKU на 1м" = sku_per_1m,
                   "Частка логіст.витрат" = logistic_share,
                   "Кіл-ть чеків" = avr_checks_qty,
                   "Частка власн.карт." = card_owners_share,
                   "Сума чеку" = avr_check_sum,
                   "Од-ць у чеку" = avr_check_units,
                   "Ціна од-ці" = avr_unit_price,
                   "Кіл-ть SKU у чеку" = avr_ckecks_sku_qty,
                   "Частка знижок та бонусів" = discounts_bonuses_share,
                   "Рентабельність прибутку" = profitibility) %>%
            arrange(subdiv_name) %>%
            pivot_longer(-subdiv_name, names_to = "index",
                         values_to = "value") %>%
            pivot_wider(names_from = "subdiv_name", values_from = "value") %>%
            rowwise() %>%
            mutate(quantile_75 = quantile(c_across(where(is.numeric)), probs = 0.75),
                   quantile_25 = quantile(c_across(where(is.numeric)), probs = 0.25)) %>%
            ungroup()
    })

    output$subdiv_table <- renderReactable({

        req(category_subdiv_tbl())

        category_subdiv_tbl() %>%
            reactable(
                striped = TRUE,
                bordered = TRUE,
                pagination = FALSE,
                highlight = TRUE,
                height = 650,
                theme = reactableTheme(headerStyle = list(backgroundColor = "#EED8AE",
                                                          fontSize = "14px"),
                                       cellStyle = list(fontFamily = "Monaco, monospace",
                                                        fontSize = "14px")
                ),
                defaultColDef = colDef(
                    style = function(value, index, name) {
                        if (name != "index") {
                            if (index %in% c(4, 5, 8, 11, 15, 16, 17, 18, 19, 20, 22)) {
                                if (value > category_subdiv_tbl()$quantile_75[index]) {
                                    color <- "green"
                                } else {
                                    color <- "red"
                                }
                            } else if (index %in% c(2, 14)) {
                                if (value < category_subdiv_tbl()$quantile_25[index]) {
                                    color <- "green"
                                } else {
                                    color <- "red"
                                }
                            } else {
                                color <- "black"
                            }
                        } else {
                            color <- "black"
                        }
                        list(color = color)
                    }
                ),
                columns = list(
                    index = colDef(name = "Показник / Категорія",
                                   minWidth = 140),
                    quantile_75 = colDef(show = FALSE),
                    quantile_25 = colDef(show = FALSE)
                )
            )
    })


    category_cheks_share_tbl <- reactive({

        req(input$trade_category_selected, rv$subdiv_category_structure)

        rv$subdiv_category_structure %>%
            left_join(rv$subdiv_trade_category,
                      by = "subdiv_id") %>%
            left_join(select(ref_subdiv, subdiv_id, subdiv_name),
                      by = "subdiv_id") %>%
            mutate(subdiv_name = str_replace(subdiv_name, "ТоргівельнийЦентр", "ТЦ"),
                   subdiv_name = str_replace(subdiv_name, "[Р|р]оздріб", ""),
                   subdiv_name = str_replace(subdiv_name, "Кам'янець-Подільський", "Кам-Под")) %>% 
            filter(trade_category %in% input$trade_category_selected) %>%
            select(subdiv_name, category_name, category_checks_share) %>%
            arrange(subdiv_name, category_name) %>%
            pivot_wider(names_from = subdiv_name,
                        values_from = category_checks_share) %>%
            mutate(across(where(is.numeric), ~replace_na(.x, 0)))
    })

    output$cheks_share_table <- renderReactable({

        req(category_cheks_share_tbl())

        category_cheks_share_tbl() %>%
            reactable(
                pagination = FALSE,
                defaultColDef = colDef(
                    cell = data_bars(category_cheks_share_tbl(),
                                     text_position = "above",
                                     fill_color = c("#22577A","#38A3A5","#57CC99","#80ED99","#C7F9CC"),
                                     number_fmt = scales::percent)
                ),
                columns = list(
                    category_name = colDef(name = "Категор.напрямок",
                                           minWidth = 140)
                )
            )
    })


    category_revenue_share_tbl <- reactive({

        req(input$trade_category_selected, rv$subdiv_category_structure)

        rv$subdiv_category_structure %>%
            left_join(rv$subdiv_trade_category,
                      by = "subdiv_id") %>%
            left_join(select(ref_subdiv, subdiv_id, subdiv_name),
                      by = "subdiv_id") %>%
            mutate(subdiv_name = str_replace(subdiv_name, "ТоргівельнийЦентр", "ТЦ"),
                   subdiv_name = str_replace(subdiv_name, "[Р|р]оздріб", ""),
                   subdiv_name = str_replace(subdiv_name, "Кам'янець-Подільський", "Кам-Под")) %>% 
            filter(trade_category %in% input$trade_category_selected) %>%
            mutate(label = paste(round(category_revenue_share*100), "%(",
                                 round(category_inventory_share*100), ")")) %>%
            select(subdiv_name, category_name, label) %>%
            arrange(subdiv_name, category_name) %>%
            pivot_wider(names_from = subdiv_name,
                        values_from = label) %>%
            mutate(across(everything(), ~replace_na(.x, "-")))
    })


     output$revenue_share_table <- renderReactable({

         req(category_revenue_share_tbl())

         category_revenue_share_tbl() %>%
             reactable(
                 striped = TRUE,
                 bordered = TRUE,
                 pagination = FALSE,
                 theme = reactableTheme(headerStyle = list(backgroundColor = "#EED8AE",
                                                           fontSize = "14px"),
                                        cellStyle = list(fontFamily = "Monaco",
                                                         fontSize = "15px")
                 ),
                 columns = list(
                     category_name = colDef(name = "Категор.напрямок",
                                            minWidth = 140)
                 )
             )
     })


     subdiv_data <- reactive({

         req(rv$result_tbl)

         rv$result_tbl %>%
             filter(subdiv_name == input$subdiv_selected) %>% 
             select(trade_category,
                    profitibility, net_profit, avr_gross_profit,
                    avr_salary_sum, stuff_units, employee_workload, gmrol,
                    area_maintenance, total_area, content_1m, avr_subdiv_share,
                    total_expenses, other_expenses, logistic_share,
                    margin, discounts_share, paid_bonus_share, revenue_generator,
                    avr_revenue, revenue_per_1m, gmros, trade_area_share,
                    avr_checks_qty, avr_check_sum, avr_check_units, avr_unit_price,
                    avr_ckecks_sku_qty,card_owners_share,
                    avr_inventory_1m, gmroi,
                    subdiv_name
             ) %>%
             bind_rows(semi_join(rv$quantile_75, ., by = "trade_category")) %>% 
             bind_rows(semi_join(rv$quantile_25, ., by = "trade_category"))
         
     })
     
     colors <- reactive({
         
         req(subdiv_data())
         
         temp <- c(subdiv_data()$margin[1] > subdiv_data()$margin[2],
           subdiv_data()$avr_check_sum[1] >= subdiv_data()$avr_check_sum[2],
           subdiv_data()$avr_check_units[1] >= subdiv_data()$avr_check_units[2],
           subdiv_data()$avr_unit_price[1] >= subdiv_data()$avr_unit_price[2],
           subdiv_data()$avr_ckecks_sku_qty[1] >= subdiv_data()$avr_ckecks_sku_qty[2],
           subdiv_data()$card_owners_share[1] >= subdiv_data()$card_owners_share[2])
         
         temp <- ifelse(temp == 0, 'Crimson', 'Black')
     })
     

     output$diagram <- renderGrViz({

         req(subdiv_data())
         
         grViz("digraph {

    # graph, node, and edge definitions
    graph [compound = true, nodesep = .5, ranksep = .25,
           color = green4]
    node [fontname = Helvetica, fontcolor = darkslategray,
          shape = rectangle, fixedsize = true, width = 1.5,
          color = darkslategray, style = filled]
    edge [color = grey, arrowhead = none, arrowtail = none]

    node [fillcolor = 'PaleGreen3']
    a

    node [fillcolor = 'PaleGreen2']
    b c 

    node [fillcolor = 'PaleGreen1']
    d e
    
    node [fillcolor = '#E8FADF']
    i j
    
    node [fillcolor = 'Honeydew']
    o p
    
    ## subgraph for R information
    subgraph cluster0 {
        node [fixedsize = true, width = 2.5]
        f[label = 'З/п - @@6\n(кіл-ть - @@7)']
        g[label = 'Заг.площа(@@8) *\nУтримання 1м2(@@9грн)']
        h[label = 'Інші - @@10']

        # edge definitions with the node IDs
        f -> g-> h
    }

    subgraph cluster1 {
        node [fixedsize = true, width = 2.5]
        k[label = 'Структура продажу\n(@@13)']
        l[label = 'Надані знижки\n(@@14%)']
        m[label = 'Використані бонуси\n(@@15%)']

        # edge definitions with the node IDs
        k -> l-> m
    }

    a[label = 'Прибуток\n@@1']
    b[label = 'Вал.прибуток\n@@2']
    c[label = 'Витрати\n@@3']
    d[label = 'Виторг\n@@4']
    e[label = 'Маржа\n@@5', fontcolor = '@@20']
    i[label = 'Кіл-ть чеків\n@@11']
    j[label = 'Середній чек\n@@12', fontcolor = '@@21']
    n[label = 'Кіл-ть од-ць\n@@16', fontcolor = '@@22']
    o[label = 'Середня ціна\n@@17', fontcolor = '@@23']
    p[label = 'Кіл-ть SKU\n@@18', fontcolor = '@@24']
    r[label = 'Кіл-ть відв-ів']
    s[label = 'Конверсія']
    q[label = 'Частка карток\n@@19', fontcolor = '@@25']

    # edge definitions with the node IDs
    a -> b
    a -> c
    b -> d
    b -> e
    c -> f [lhead = cluster0]
    d -> j
    d -> i
    e -> k [lhead = cluster1]
    i -> r
    i -> s
    j -> n
    j -> o
    n -> p
    p -> q
    s -> q
    
    }
    [1]:format(subdiv_data()$net_profit[1], big.mark = '`')
    [2]:format(subdiv_data()$avr_gross_profit[1], big.mark = '`')
    [3]:format(subdiv_data()$total_expenses[1], big.mark = '`')
    [4]:format(subdiv_data()$avr_revenue[1], big.mark = '`')
    [5]:scales::percent(subdiv_data()$margin[1])
    [6]:subdiv_data()$avr_salary_sum[1]
    [7]:subdiv_data()$stuff_units[1]
    [8]:subdiv_data()$total_area[1]
    [9]:subdiv_data()$content_1m[1]
    [10]:subdiv_data()$other_expenses[1]
    [11]:subdiv_data()$avr_checks_qty[1]
    [12]:subdiv_data()$avr_check_sum[1]
    [13]:subdiv_data()$revenue_generator[1]
    [14]:subdiv_data()$discounts_share[1]*100
    [15]:subdiv_data()$paid_bonus_share[1]*100
    [16]:subdiv_data()$avr_check_units[1]
    [17]:subdiv_data()$avr_unit_price[1]
    [18]:subdiv_data()$avr_ckecks_sku_qty[1]
    [19]:scales::percent(subdiv_data()$card_owners_share[1])
    [20]:colors()[1]
    [21]:colors()[2]
    [22]:colors()[3]
    [23]:colors()[4]
    [24]:colors()[5]
    [25]:colors()[6]
    ")

     })
     
     
     card_owners_tbl <- reactive({
         
         req(rv$subdiv_cards_data)
         
         rv$subdiv_cards_data %>%
             filter(trade_category == input$trade_category_selected) %>% 
             select(-trade_category) %>%
             rename("Частка власн.карток" = card_owners_share,
                    "Загальна кіль-ть" = total_purch,
                    "Частка >1 покупки" = purch_more_1_share,
                    "Кіл-ть нових" = new_cards) %>% 
             pivot_longer(-subdiv_name, names_to = "index",
                          values_to = "value") %>%
             pivot_wider(names_from = "subdiv_name", values_from = "value") %>%
             rowwise() %>%
             mutate(quantile_75 = quantile(c_across(where(is.numeric)), probs = 0.75)) %>%
             ungroup()
         
     }) 
     
     output$card_owners_table <- renderReactable({
         
         req(card_owners_tbl())
         
         card_owners_tbl() %>% 
             reactable(
                 striped = TRUE,
                 bordered = TRUE,
                 pagination = FALSE,
                 theme = reactableTheme(headerStyle = list(backgroundColor = "#EED8AE",
                                                           fontSize = "14px"),
                                        cellStyle = list(fontFamily = "Source Code Pro, monospace", 
                                                         fontSize = "14px")
                 ),
                 defaultColDef = colDef(
                     style = function(value, index, name) {
                         if (name != "index") {
                             if (index %in% c(1, 3, 4)) {
                                 if (value > card_owners_tbl()$quantile_75[index]) {
                                     color <- "green"
                                 } else {
                                     color <- "red"
                                 }
                             } else {
                                 color <- "black"
                             }
                         } else {
                             color <- "black"
                         }
                         list(color = color)
                     }
                 ),
                 columns = list(
                     index = colDef(name = "Показник / Категорія",
                                    minWidth = 140),
                     quantile_75 = colDef(show = FALSE)
                 )
             )
         
     })
     
     
     proposal_table_data <- reactive({
         
         req(subdiv_data(), rv$quarter_data)
         
         tibble(name = character(), condition = logical()) %>% 
             add_row(
                 name = "Низьки показники - розглянути можливість зменшення торгівельної площі",
                 condition = with(subdiv_data(),
                                  revenue_per_1m[1] < revenue_per_1m[2] &
                                      gmros[1] < gmros[2] &
                                      gmroi[1] < gmroi[2] &
                                      profitibility[1] < 0)) %>%
             add_row(
                 name = "Визначити причину низької частки торгівельної площі",
                 condition = subdiv_data()$trade_area_share[1] < 0.8) %>% 
             
             add_row(
                 name = "Низькі залишки - переглянути схему розміщення торг.обладнання",
                 condition = subdiv_data()$avr_inventory_1m[1] < subdiv_data()$avr_inventory_1m[3]) %>% 
             
             add_row(
                 name = "Низький GMROI - оптимізувати асортимент магазину",
                 condition = with(subdiv_data(),
                                  gmroi[1] <= gmroi[3] &
                                      profitibility[1] > profitibility[3])) %>%

             add_row(
                 name = "Невідповідність витрат на утримання потоку покупців - залучити більше покупців",
                 condition = with(subdiv_data(),
                                  content_1m[1] > content_1m[2] &
                                      avr_checks_qty[1] < avr_checks_qty[2] &
                                      profitibility[1] < profitibility[3])) %>%

             add_row(
                 name = "Значна частка логістичгих витрат - переглянути логістичну схему підрозділу",
                 condition = subdiv_data()$logistic_share[1] > subdiv_data()$logistic_share[2]) %>%

             add_row(
                 name = "Низькі навантаженність та GMROL - розглянути можливість зменшення персоналу",
                 condition = with(subdiv_data(),
                                  employee_workload[1] < employee_workload[3] &
                                      gmrol[1] < gmrol[3])) %>%

             add_row(
                 name = "Значна частка В2В відвантажень - розглянути можливість не викладати В2В-асортимент на полиці",
                 condition = subdiv_data()$avr_subdiv_share[1] < 0.8) %>%

             add_row(
                 name = str_glue("Визначити причину низької частки бонусних карток та збільшити її 
                                  (ціл.зн-ня {subdiv_data()$card_owners_share[2]})"),
                 condition = subdiv_data()$card_owners_share[1] < subdiv_data()$card_owners_share[2]) %>%

             add_row(
                 name = "Покращити роботу персоналу магазину по роботі з покупцями (cross-sell - продаж супутніх товарів)",
                 condition = with(subdiv_data(),
                                  avr_check_sum[1] < avr_check_sum[2] &
                                      avr_check_units[1] < avr_check_units[2] &
                                      avr_ckecks_sku_qty[1] < avr_ckecks_sku_qty[2])) %>%
             
             add_row(
                 name = paste0("Збільшити кількість одиниць у чеку до ",
                               round(subdiv_data()$avr_check_units[2] * 
                                   rv$quarter_data$check_units_growth[
                                       rv$quarter_data$trade_category == input$trade_category_selected],
                                   2)),
                 condition = subdiv_data()$avr_check_units[1] < subdiv_data()$avr_check_units[2]) %>%
             
             add_row(
                 name = paste0("Збільшити кількість SKU у чеку до ",
                               round(subdiv_data()$avr_ckecks_sku_qty[2] * 
                                         rv$quarter_data$check_sku_qty_growth[
                                             rv$quarter_data$trade_category == input$trade_category_selected],
                                     2)),
                 condition = subdiv_data()$avr_ckecks_sku_qty[1] < subdiv_data()$avr_ckecks_sku_qty[2]) %>%

             add_row(
                 name = "Покращити роботу персоналу магазину по роботі з покупцями (upsell - продаж якісніших товарів)",
                 condition = with(subdiv_data(),
                                  avr_check_sum[1] < avr_check_sum[2] &
                                      avr_unit_price[1] < avr_unit_price[2])) %>%
             
             add_row(
                 name = paste0("Збільшити ціну одиниці у чеку до ",
                               round(subdiv_data()$avr_unit_price[2] * 1.05,
                                     2)),
                 condition = subdiv_data()$avr_unit_price[1] < subdiv_data()$avr_unit_price[2]) %>%
             
             filter(condition == TRUE) %>%
             select("Завдання" = name)
     })
     
     # Define the table output
     output$proposal_table <- DT::renderDataTable(
         proposal_table_data(),
         extensions = c('Buttons'),
         options = list(
                        dom = 'Bt',
                        buttons = 'excel'
                       )
     )

}

# Run the application 
shinyApp(ui = ui, server = server)
