use base64::{engine::general_purpose, Engine};
use chrono::NaiveDate;
use colored::Colorize;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str;
use tokio::time::{sleep, Duration};
use tokio_postgres::NoTls;

//Структура для разбора JSON на запрос новой марки с шлюза
#[derive(Deserialize)]
struct GetMarksStruct {
    marks: Vec<HashMap<String, String>>,
}

//Структура для сборки json на отправку
#[derive(Serialize)]
struct RolloutMarksStruct {
    marks: Vec<Mark>,
    page_size: i64,
}

#[derive(Serialize)]
struct Mark {
    proddate: String,
    code: String,
    #[serde(rename = "type")]
    type_field: i32,
}

//Структура для разбора JSON принятых шлюзом марок
#[derive(Deserialize)]
struct RolloutResult {
    marks: Vec<HashMap<String, String>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    loop {
        ////////////////////////////////
        // Подключаемся к базе данных

        let (client, connection) =
            tokio_postgres::connect("postgresql://test:test@127.0.0.1/test_bd", NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        println!("\n{}", "######## Получение марок из шлюза".yellow());
        println!(
            "{}",
            "Запрашиваем gtin продуктов, для которых нужно запросить марки".green()
        );

        //Запрашиваем gtin продуктов, для которых нужно запросить марки
        let rows = client
            .query(
                "-- 1 Выбрать gtin продукта, которые нужно загружать со шлюза
                        select
                            gtin,
                            description,
                            how_many_code_store
                        from
                            goods
                        where
                            get_codes = true",
                &[],
            )
            .await?;

        //Обрабатываем ответ на запрос gtin продуктов, для которых нужно запросить марки
        for row in rows {
            //Считываем gtin и описание продукта в переменные
            let gtin: &str = row.get("gtin");
            let how_many_code_store: i32 = row.get("how_many_code_store");
            let description: &str = row.get("description");

            println!("{}", "Для продукта:".cyan());
            println!("{}: {}", gtin, description);
            println!("Задано хранить: {} кодов", how_many_code_store);
            print!("{}", "    Доступно для печати кодов... ".cyan());

            //Делаем запрос количеста кодов доступных для печати для этого продукта
            let rows = client
            .query("-- 2 Выбрать коды, которые не отправлялись для печати, а значит доступн для печати
                    select count(code_id) from	codes 
                    where good_id = (select good_id from goods where gtin = $1::TEXT) and sended_to_aw is null", &[&gtin])
            .await?;

            let free_codes_for_print: i64 = rows[0].get("count");
            println!("{}", free_codes_for_print);

            //Проверяем, надо ли для этого продукта запрашивать марки
            if free_codes_for_print < how_many_code_store as i64 {
                println!(
                    "{}",
                    "    У этого продукта мало кодов! надо запросить новые".cyan()
                );

                //Запрашиваем марки для этого продукта из шлюза
                //Делаем запрос марок с сервера

                //Вычисляем, сколько кодов запросить
                let mut how_many_codes_get = how_many_code_store as i64 - free_codes_for_print;
                if how_many_codes_get > 10 {
                    how_many_codes_get = 10;
                }

                let url = format!(
                    "http://192.168.10.203/exchangemarks/hs/api/getmarks?gtin={gtin}&limit={}",
                    how_many_codes_get
                );
                println!("{} {}", "    Делаем запрос марок с сервера: ".cyan(), url);

                let resp = match reqwest::get(url).await {
                    Ok(resp) => resp,
                    Err(error) => {
                        print!("{}", "ERROR: ".red());
                        println!("{}", error);
                        continue;
                    }
                };

                //Парсим JSON
                let jsn: GetMarksStruct = match resp.json().await {
                    Ok(jsn) => jsn,
                    Err(error) => {
                        print!("{}", "ERROR: ".red());
                        println!("{}", error);
                        continue;
                    }
                };

                println!("{}", "        Шлюз прислал коды: ".cyan());

                //Для каждого кода в jsone'е ..
                for hm in jsn.marks {
                    let code_in_base64 = hm.get("code").unwrap();
                    println!("{} {}", "        Код в base64".bold(), code_in_base64);

                    let code = general_purpose::STANDARD.decode(&code_in_base64).unwrap();
                    let code = str::from_utf8(&code).unwrap();

                    println!("{} {}", "        Код декодированный".bold(), code);

                    //Парсим декодированный код. заодно проверяется его корректность
                    //^01\d{14}21.{6}.93.{4}$
                    let re =
                        Regex::new(r"^01(?P<gtin>\d{14})21(?P<serial>.{6}).93(?P<crypto>.{4})$")
                            .unwrap();
                    let re = re.captures(code).unwrap();

                    let gtin = &re["gtin"];
                    let serial = &re["serial"];
                    let crypto = &re["crypto"];

                    println!(
                        "        {} gtin: {} serial: {} crypto: {}",
                        "Код распарсенный".bold(),
                        gtin,
                        serial,
                        crypto
                    );

                    //Добавляем код в базу
                    print!("            {}", "Добавляем этот код в базу... ".cyan());
                    let rows = client
                        .query(
                            "insert
                                into codes (good_id, serial, crypto, loaded_from_gate)
                            values (
                                (select good_id from goods where gtin = $1::TEXT),
                                $2::TEXT,
                                $3::TEXT,
                                now())
                            returning code_id;",
                            &[&gtin, &serial, &crypto],
                        )
                        .await?;

                    let code_id: i64 = rows[0].get("code_id");
                    println!("{} code_id: {} \n", "успешно.".green(), code_id);
                }
            } else {
                println!(
                    "{}",
                    "    У этого продукта хватает кодов, не запрашиваю новые\n".cyan()
                );
            }
        }

        /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        println!("{}", "@@@@@@@@@ Выгрузка марок в шлюз".magenta());
        println!(
            "{}",
            "Запрашиваем gtin продуктов, для которых нужно выгружать марки в шлюз".cyan()
        );

        //Запрашиваем gtin продуктов, для которых нужно запросить марки
        let products = client
                .query("-- 1 Выбрать gtin продукта, которые нужно выгружать в шлюз
                        select gtin, description, how_many_code_store from goods where upload_codes = true", &[])
                .await?;

        //Обрабатываем ответ на запрос gtin продуктов, для которых нужно запросить марки
        for product in products {
            //Считываем gtin и описание продукта в переменные
            let gtin: &str = product.get("gtin");
            let description: &str = product.get("description");

            print!(
                "{} {} {} :\n",
                "    Коды на выгрузку для".cyan(),
                gtin,
                description
            );

            //Делаем запрос 100 кодов, готовых для выгрузки
            let codes = client
                .query(
                    "select serial, crypto, prod_date, codetype from codes where 
                        good_id = (select good_id from goods where gtin = $1::TEXT)
                        and prod_date is not null 
                        and sended_to_gate is null
                    limit 100",
                    &[&gtin],
                )
                .await?;

            if codes.is_empty() {
                println!("    {}\n", "Нет кодов на выгрузку".cyan());
                continue;
            }

            //Хранит марки,
            let mut marks: Vec<Mark> = Vec::new();

            //Парсим полученные коды на выгрузку из БД
            for code in codes {
                let serial: &str = code.get("serial");
                let crypto: &str = code.get("crypto");
                let proddate: NaiveDate = code.get("prod_date");
                let proddate = proddate.format("%Y%m%d").to_string();
                let code_type: i32 = code.get("codetype");

                print!("        {} {} {} {} от {}", gtin, serial, crypto, code_type, &proddate);

                let code_full_format =
                    format!("01{}21{}{}93{}", gtin, serial, 0x1D as char, crypto);
                print!("{} {:?}", " Полный формат:".cyan(), code_full_format);

                let code_in_base64 = general_purpose::STANDARD.encode(&code_full_format);
                println!("{} {}", " base64:".cyan(), code_in_base64);

                let mark = Mark {
                    proddate: proddate,
                    code: code_in_base64,
                    type_field: code_type,
                };
                marks.push(mark);
            }

            let count = marks.len();
            println!("        {} {}", "Итого кодов:".cyan(), count);

            println!("        {}", "Формирую JSON ...".cyan());
            let rollout_marks_struct = RolloutMarksStruct {
                marks: marks,
                page_size: count as i64,
            };
            let rollout_marks_json = serde_json::to_string_pretty(&rollout_marks_struct).unwrap();

            //Передаем марки в шлюз
            let url = format!("http://192.168.10.203/exchangemarks/hs/api/rollout?gtin={gtin}");
            print!("        {} {} ... ", "    Передаю в шлюз: ".cyan(), url);

            let http_client = reqwest::Client::new();
            let resp = match http_client.post(url).body(rollout_marks_json).send().await {
                Ok(resp) => resp,
                Err(error) => {
                    print!("{}", "ERROR: ".red());
                    println!("{}", error);
                    continue;
                }
            };

            println!("{}", "Успешно".green());
            let result_json = &*resp.text().await.unwrap();
            println!("            {}\n {}", "Ответ сервера:".cyan(), result_json);

            println!("        {}", "Парсинг ответа".cyan());
            let rollout_result: RolloutResult = serde_json::from_str(result_json).unwrap();

            for code in rollout_result.marks {
                let code_in_base64 = code.get("code").unwrap();
                let result = code.get("result").unwrap();
                print!(
                    "            {} {} {} {}",
                    "Код: ".cyan(),
                    code_in_base64,
                    "Результат:".cyan(),
                    result
                );

                //Если код принят сервером, то пишем в БД, что код выгружен
                if result == "ok" {
                    let code = general_purpose::STANDARD.decode(&code_in_base64).unwrap();
                    let code = str::from_utf8(&code).unwrap();

                    print!("\n                {} {}", "Декодированный:".cyan(), code);

                    //Парсим декодированный код. заодно проверяется его корректность
                    //^01\d{14}21.{6}.93.{4}$
                    let re =
                        Regex::new(r"^01(?P<gtin>\d{14})21(?P<serial>.{6}).93(?P<crypto>.{4})$")
                            .unwrap();
                    let re = re.captures(code).unwrap();

                    let gtin = &re["gtin"];
                    let serial = &re["serial"];
                    let crypto = &re["crypto"];

                    println!(
                        " {} gtin: {} serial: {} crypto: {}",
                        "Распарсенный:".cyan(),
                        gtin,
                        serial,
                        crypto
                    );

                    println!(
                        "                {}",
                        "Записываю в базу, что код принят шлюзом".cyan()
                    );

                    //Добавляем код в базу
                    print!("                {}", "Добавляем этот код в базу... ".cyan());
                    let rows = client.query("-- Сделать запись в БД, что код выгружен в шлюз
                                            update
                                                codes
                                            set
                                                sended_to_gate = now()
                                            where
                                                good_id = (select good_id from goods where gtin = $1::TEXT)
                                                and serial = $2::TEXT
                                                and crypto = $3::TEXT
                                            returning code_id,
                                                serial,
                                                crypto", &[&gtin, &serial, &crypto]).await?;

                    let code_id: i64 = rows[0].get("code_id");
                    println!("{} code_id: {} \n", "успешно.".green(), code_id);
                } else {
                    println!(" {}", "ERROR: Код не принят".red());
                }
            }
        }

        sleep(Duration::from_secs(1)).await;
        //Конец loop
    }
    Ok(())
}
