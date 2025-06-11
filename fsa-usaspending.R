library(magrittr)
library(tidyverse)
library(furrr)
library(openxlsx2)
library(archive)
library(future.callr)
library(arrow)

update_payments <- FALSE

if(update_payments){
  
  get_lastest_usaspending <-
    function(){
      
      bucket_list <-
        xml2::read_xml("https://files.usaspending.gov/award_data_archive/?list-type=2") %>%
        xml2::as_list() %$%
        ListBucketResult
      
      files <-
        bucket_list[names(bucket_list) == "Contents"] %>%
        unname() %>%
        purrr::list_transpose(simplify = TRUE) %>%
        purrr::map(unlist) %>%
        tibble::as_tibble()
      
      while("NextContinuationToken" %in% names(bucket_list)){
        bucket_list <-
          xml2::read_xml(paste0("https://files.usaspending.gov/award_data_archive/?list-type=2&continuation-token=", htmltools::urlEncodePath(bucket_list$NextContinuationToken[[1]]))) %>%
          xml2::as_list() %$%
          ListBucketResult
        
        files <-
          dplyr::bind_rows(files,
                           bucket_list[names(bucket_list) == "Contents"] %>%
                             unname() %>%
                             purrr::list_transpose(simplify = TRUE) %>%
                             purrr::map(unlist) %>%
                             tibble::as_tibble()
          )
      }
      
      return(readr::type_convert(files))
    }
  
  latest_usda_usaspending <-
    get_lastest_usaspending() %>%
    dplyr::filter(stringr::str_detect(Key, "_012_Assistance_Full_"))
  
  parse_usaspending <-
    function(x){
      outfile <- 
        x %>%
        basename() %>%
        tools::file_path_sans_ext() %>%
        paste0(".csv") %>%
        file.path("fsa-usaspending", "raw", .)
      
      if(file.exists(outfile))
        return(outfile)
      
      usaspending_zip <- tempfile(fileext = ".zip")
      
      curl::curl_download(x,
                          destfile = usaspending_zip)
      
      archive::archive(usaspending_zip)$path %>%
        purrr::map_dfr(
          \(y){
            archive::archive_read(usaspending_zip, file = y) %>%
              readr::read_csv(col_types = readr::cols(.default = "c")) %>%
              dplyr::filter(awarding_sub_agency_name == "Farm Service Agency")
          }
        ) %>%
        readr::write_csv(outfile)
      
      return(outfile)
    }
  
  plan(callr, workers = 8)
  
  fsa_usaspending <-
    latest_usda_usaspending$Key %>%
    file.path("https://files.usaspending.gov/award_data_archive", .) %>%
    furrr::future_map(parse_usaspending)
  
  plan(sequential)
  
  
  out <-
    arrow::open_dataset(
      "fsa-usaspending/raw",
      format = "csv",
      skip = 1,
      schema = 
        arrow::schema(
          assistance_transaction_unique_key = string(),
          assistance_award_unique_key = string(),
          award_id_fain = string(),
          modification_number = string(),
          award_id_uri = string(),
          sai_number = string(),
          federal_action_obligation = float64(),
          total_obligated_amount = float64(),
          total_outlayed_amount_for_overall_award = float64(),
          indirect_cost_federal_share_amount = float64(),
          non_federal_funding_amount = float64(),
          total_non_federal_funding_amount = float64(),
          face_value_of_loan = float64(),
          original_loan_subsidy_cost = float64(),
          total_face_value_of_loan = float64(),
          total_loan_subsidy_cost = float64(),
          generated_pragmatic_obligations = float64(),
          disaster_emergency_fund_codes_for_overall_award = string(),
          `outlayed_amount_from_COVID-19_supplementals_for_overall_award` = float64(),
          `obligated_amount_from_COVID-19_supplementals_for_overall_award` = float64(),
          outlayed_amount_from_IIJA_supplemental_for_overall_award = float64(),
          obligated_amount_from_IIJA_supplemental_for_overall_award = float64(),
          action_date = date32(),
          action_date_fiscal_year = int16(),
          period_of_performance_start_date = date32(),
          period_of_performance_current_end_date = date32(),
          awarding_agency_code = string(),
          awarding_agency_name = string(),
          awarding_sub_agency_code = string(),
          awarding_sub_agency_name = string(),
          awarding_office_code = string(),
          awarding_office_name = string(),
          funding_agency_code = string(),
          funding_agency_name = string(),
          funding_sub_agency_code = string(),
          funding_sub_agency_name = string(),
          funding_office_code = string(),
          funding_office_name = string(),
          treasury_accounts_funding_this_award = string(),
          federal_accounts_funding_this_award = string(),
          object_classes_funding_this_award = string(),
          program_activities_funding_this_award = string(),
          recipient_uei = string(),
          recipient_duns = string(),
          recipient_name = string(),
          recipient_name_raw = string(),
          recipient_parent_uei = string(),
          recipient_parent_duns = string(),
          recipient_parent_name = string(),
          recipient_parent_name_raw = string(),
          recipient_country_code = string(),
          recipient_country_name = string(),
          recipient_address_line_1 = string(),
          recipient_address_line_2 = string(),
          recipient_city_code = string(),
          recipient_city_name = string(),
          prime_award_transaction_recipient_county_fips_code = string(),
          recipient_county_name = string(),
          prime_award_transaction_recipient_state_fips_code = string(),
          recipient_state_code = string(),
          recipient_state_name = string(),
          recipient_zip_code = string(),
          recipient_zip_last_4_code = string(),
          prime_award_transaction_recipient_cd_original = string(),
          prime_award_transaction_recipient_cd_current = string(),
          recipient_foreign_city_name = string(),
          recipient_foreign_province_name = string(),
          recipient_foreign_postal_code = string(),
          primary_place_of_performance_scope = string(),
          primary_place_of_performance_country_code = string(),
          primary_place_of_performance_country_name = string(),
          primary_place_of_performance_code = string(),
          primary_place_of_performance_city_name = string(),
          prime_award_transaction_place_of_performance_county_fips_code = string(),
          primary_place_of_performance_county_name = string(),
          prime_award_transaction_place_of_performance_state_fips_code = string(),
          primary_place_of_performance_state_name = string(),
          primary_place_of_performance_zip_4 = string(),
          prime_award_transaction_place_of_performance_cd_original = string(),
          prime_award_transaction_place_of_performance_cd_current = string(),
          primary_place_of_performance_foreign_location = string(),
          cfda_number = string(),
          cfda_title = string(),
          funding_opportunity_number = string(),
          funding_opportunity_goals_text = string(),
          assistance_type_code = string(),
          assistance_type_description = string(),
          transaction_description = string(),
          prime_award_base_transaction_description = string(),
          business_funds_indicator_code = string(),
          business_funds_indicator_description = string(),
          business_types_code = string(),
          business_types_description = string(),
          correction_delete_indicator_code = string(),
          correction_delete_indicator_description = string(),
          action_type_code = string(),
          action_type_description = string(),
          record_type_code = string(),
          record_type_description = string(),
          highly_compensated_officer_1_name = string(),
          highly_compensated_officer_1_amount = string(),
          highly_compensated_officer_2_name = string(),
          highly_compensated_officer_2_amount = string(),
          highly_compensated_officer_3_name = string(),
          highly_compensated_officer_3_amount = string(),
          highly_compensated_officer_4_name = string(),
          highly_compensated_officer_4_amount = string(),
          highly_compensated_officer_5_name = string(),
          highly_compensated_officer_5_amount = string(),
          usaspending_permalink = string(),
          initial_report_date = date32(),
          last_modified_date = date32()
        )
    ) |>
    dplyr::select(
      YEAR = action_date_fiscal_year,
      COUNTY_FIPS = prime_award_transaction_place_of_performance_county_fips_code,
      dplyr::everything()
    ) |>
    dplyr::arrange(YEAR,
                   COUNTY_FIPS,
                   cfda_number) |>
    dplyr::collect()
  
  gc();gc()
  
  out |>
    dplyr::group_by(YEAR, 
                    COUNTY_FIPS) |>
    arrow::write_dataset(path = "fsa-usaspending/parquet/BY_YEAR",
                         format = "parquet",
                         existing_data_behavior = "delete_matching",
                         version = "latest",
                         max_partitions = 50000L,
                         max_open_files = 50000L)
  
  gc();gc()
  
  # arrow::open_dataset("fsa-usaspending/parquet/BY_YEAR") |>
  out |>
    dplyr::group_by(COUNTY_FIPS,
                    YEAR) |>
    arrow::write_dataset(path = "fsa-usaspending/parquet/BY_COUNTY",
                         format = "parquet",
                         existing_data_behavior = "delete_matching",
                         version = "latest",
                         max_partitions = 50000L,
                         max_open_files = 50000L)
  
  gc();gc()
  
  rm(out)
  
  gc();gc()
  
  readr::read_csv("https://falextracts.s3.amazonaws.com/Assistance%20Listings/usaspendinggov/2024/12-Dec/AssistanceListings_USASpendingGov_PUBLIC_WEEKLY_20241228.csv",
                  col_types = cols(.default = col_character())
  ) %>%
    dplyr::rename(cfda_number = `Program Number`,
                  cfda_title = `Program Title`
                  ) %>%
    readr::write_csv("fsa-usaspending/usaspending_assistance_listings.csv")
  
  aws_s3 <-
    paws::s3(credentials = 
               list(creds = list(
                 access_key_id = keyring::key_get("aws_access_key_id"),
                 secret_access_key = keyring::key_get("aws_secret_access_key")
               )))
  
  uploads <-
    list.files("fsa-usaspending",
               full.names = TRUE,
               recursive = TRUE) %>%
    furrr::future_map(\(x){
      tryCatch(aws_s3$put_object(Bucket = "sustainable-fsa",
                                 Body = x,
                                 Key = x,
                                 ChecksumSHA256 = file(x) %>% 
                                   openssl::sha256() %>%
                                   openssl::base64_encode()),
               error = function(e){return(NULL)})
      
    },
    .options = furrr::furrr_options(seed = TRUE))
  
  plan(sequential)
  
}

fsa_payments <-
  arrow::open_dataset("fsa-usaspending/parquet/BY_COUNTY/")

fsa_payments %>%
  filter(YEAR %in% 2008:2023,
         `cfda_number` %in% c("10.089", "10.109")) %>%
  dplyr::select(COUNTY_FIPS, YEAR, 
                federal_action_obligation, total_obligated_amount, total_outlayed_amount_for_overall_award, 
                # disaster_emergency_fund_codes_for_overall_award, 
                action_date, period_of_performance_start_date, period_of_performance_current_end_date, 
                # awarding_agency_name, awarding_sub_agency_name, awarding_office_name,
                # funding_agency_name, funding_sub_agency_name, funding_office_name,
                # treasury_accounts_funding_this_award, federal_accounts_funding_this_award, 
                primary_place_of_performance_state_name, primary_place_of_performance_county_name,
                cfda_number, cfda_title, 
                # assistance_type_description,
                # transaction_description, action_type_description, 
                usaspending_permalink) %>%
  # dplyr::group_by(YEAR, `cfda_number`) %>%
  # dplyr::summarise(`federal_action_obligation` = sum(`federal_action_obligation`, na.rm = TRUE),
  #                  `total_outlayed_amount_for_overall_award` = sum(`total_outlayed_amount_for_overall_award`, na.rm = TRUE)) %>%
  collect() %>%
  dplyr::arrange(YEAR, `cfda_number`) %>%
  readr::write_csv("LFP_USAPENDING.csv")
  
  
  print(n = 700)

fsa_payments %>%
  dplyr::filter(`cfda_number` == "10.073") %>%
  dplyr::group_by(`action_date_fiscal_year`, `cfda_number`, `cfda_title`) %>%
  dplyr::summarise(`federal_action_obligation` = sum(`federal_action_obligation`, na.rm = TRUE)) %>%
  collect()

listings <-
  fsa_payments %>%
  dplyr::select(cfda_number) %>%
  dplyr::distinct() %>%
  dplyr::collect()


readr::read_csv("fsa-usaspending/usaspending_assistance_listings.csv") %>%
  dplyr::filter(stringr::str_detect(cfda_title, "Livestock Forage")) %$%
  cfda_number


fsa_payments %>%
  dplyr::select(`Accounting Program Code`, `Accounting Program Description`) %>%
  dplyr::distinct() %>%
  collect() %>%
  dplyr::arrange(`Accounting Program Code`, `Accounting Program Description`) %>%
  readr::write_csv("fsa-accounting-programs.csv")

