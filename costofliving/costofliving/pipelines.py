# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import os
import sqlite3

from itemadapter import ItemAdapter


class CostoflivingPipeline:
    def process_item(self, item, spider):
        return item


class SQLitePipeline:
    def open_spider(self, spider):
        self.connection = sqlite3.connect("numbeo.db")
        self.cursor = self.connection.cursor()
        with open(os.path.join(os.getcwd(), "costofliving", "sqls", "create.sql"), 'r', encoding='utf-8') as file:
            create_sql_query = file.read()
        self.cursor.execute(create_sql_query)
        self.connection.commit()

    def close_spider(self, spider):
        self.connection.close()

    def process_item(self, item, spider):
        with open(os.path.join(os.getcwd(), "costofliving", "sqls", "insert.sql"), 'r', encoding='utf-8') as file:
            insert_sql_query = file.read()
        self.cursor.execute(insert_sql_query, (
            item.get("today"),
            item.get("city"),
            item.get("province"),
            item.get("country"),
            item.get("continent"),
            item.get("meal__inexpensive_restaurant"),
            item.get("meal__inexpensive_restaurant_pr"),
            item.get("meal_for_2_people__mid_range_restaurant__three_course"),
            item.get("meal_for_2_people__mid_range_restaurant__three_course_pr"),
            item.get("mcmeal_at_mcdonalds_or_equivalent_combo_meal"),
            item.get("mcmeal_at_mcdonalds_or_equivalent_combo_meal_pr"),
            item.get("domestic_beer_0_5_liter_draught"),
            item.get("domestic_beer_0_5_liter_draught_pr"),
            item.get("imported_beer_0_33_liter_bottle"),
            item.get("imported_beer_0_33_liter_bottle_pr"),
            item.get("cappuccino_regular"),
            item.get("cappuccino_regular_pr"),
            item.get("cokepepsi_0_33_liter_bottle"),
            item.get("cokepepsi_0_33_liter_bottle_pr"),
            item.get("water_0_33_liter_bottle"),
            item.get("water_0_33_liter_bottle_pr"),
            item.get("milk_regular__1_liter"),
            item.get("milk_regular__1_liter_pr"),
            item.get("loaf_of_fresh_white_bread_500g"),
            item.get("loaf_of_fresh_white_bread_500g_pr"),
            item.get("rice_white__1kg"),
            item.get("rice_white__1kg_pr"),
            item.get("eggs_regular_12"),
            item.get("eggs_regular_12_pr"),
            item.get("local_cheese_1kg"),
            item.get("local_cheese_1kg_pr"),
            item.get("chicken_fillets_1kg"),
            item.get("chicken_fillets_1kg_pr"),
            item.get("buffalo_round_1kg_or_equivalent_back_leg_red_meat"),
            item.get("buffalo_round_1kg_or_equivalent_back_leg_red_meat_pr"),
            item.get("apples_1kg"),
            item.get("apples_1kg_pr"),
            item.get("banana_1kg"),
            item.get("banana_1kg_pr"),
            item.get("oranges_1kg"),
            item.get("oranges_1kg_pr"),
            item.get("tomato_1kg"),
            item.get("tomato_1kg_pr"),
            item.get("potato_1kg"),
            item.get("potato_1kg_pr"),
            item.get("onion_1kg"),
            item.get("onion_1kg_pr"),
            item.get("lettuce_1_head"),
            item.get("lettuce_1_head_pr"),
            item.get("water_1_5_liter_bottle"),
            item.get("water_1_5_liter_bottle_pr"),
            item.get("bottle_of_wine_mid_range"),
            item.get("bottle_of_wine_mid_range_pr"),
            item.get("domestic_beer_0_5_liter_bottle"),
            item.get("domestic_beer_0_5_liter_bottle_pr"),
            item.get("cigarettes_20_pack_marlboro"),
            item.get("cigarettes_20_pack_marlboro_pr"),
            item.get("one_way_ticket_local_transport"),
            item.get("one_way_ticket_local_transport_pr"),
            item.get("monthly_pass_regular_price"),
            item.get("monthly_pass_regular_price_pr"),
            item.get("taxi_start_normal_tariff"),
            item.get("taxi_start_normal_tariff_pr"),
            item.get("taxi_1km_normal_tariff"),
            item.get("taxi_1km_normal_tariff_pr"),
            item.get("taxi_1hour_waiting_normal_tariff"),
            item.get("taxi_1hour_waiting_normal_tariff_pr"),
            item.get("gasoline_1_liter"),
            item.get("gasoline_1_liter_pr"),
            item.get("volkswagen_golf_1_4_90_kw_trendline_or_equivalent_new_car"),
            item.get("volkswagen_golf_1_4_90_kw_trendline_or_equivalent_new_car_pr"),
            item.get("toyota_corolla_sedan_1_6l_97kw_comfort_or_equivalent_new_car"),
            item.get("toyota_corolla_sedan_1_6l_97kw_comfort_or_equivalent_new_car_pr"),
            item.get("basic_electricity__heating__cooling__water__garbage_for_85m2_apartment"),
            item.get("basic_electricity__heating__cooling__water__garbage_for_85m2_apartment_pr"),
            item.get("mobile_phone_monthly_plan_with_calls_and_10gb_plus_data"),
            item.get("mobile_phone_monthly_plan_with_calls_and_10gb_plus_data_pr"),
            item.get("internet_60_mbps_or_more__unlimited_data__cableadsl"),
            item.get("internet_60_mbps_or_more__unlimited_data__cableadsl_pr"),
            item.get("fitness_club__monthly_fee_for_1_adult"),
            item.get("fitness_club__monthly_fee_for_1_adult_pr"),
            item.get("tennis_court_rent_1_hour_on_weekend"),
            item.get("tennis_court_rent_1_hour_on_weekend_pr"),
            item.get("cinema__international_release__1_seat"),
            item.get("cinema__international_release__1_seat_pr"),
            item.get("preschool_or_kindergarten__full_day__private__monthly_for_1_child"),
            item.get("preschool_or_kindergarten__full_day__private__monthly_for_1_child_pr"),
            item.get("international_primary_school__yearly_for_1_child"),
            item.get("international_primary_school__yearly_for_1_child_pr"),
            item.get("one_pair_of_jeans_levis_501_or_similar"),
            item.get("one_pair_of_jeans_levis_501_or_similar_pr"),
            item.get("one_summer_dress_in_a_chain_store_zara__hnm_"),
            item.get("one_summer_dress_in_a_chain_store_zara__hnm__pr"),
            item.get("one_pair_of_nike_running_shoes_mid_range"),
            item.get("one_pair_of_nike_running_shoes_mid_range_pr"),
            item.get("one_pair_of_men_leather_business_shoes"),
            item.get("one_pair_of_men_leather_business_shoes_pr"),
            item.get("apartment_1_bedroom_in_city_centre"),
            item.get("apartment_1_bedroom_in_city_centre_pr"),
            item.get("apartment_1_bedroom_outside_of_centre"),
            item.get("apartment_1_bedroom_outside_of_centre_pr"),
            item.get("apartment_3_bedrooms_in_city_centre"),
            item.get("apartment_3_bedrooms_in_city_centre_pr"),
            item.get("apartment_3_bedrooms_outside_of_centre"),
            item.get("apartment_3_bedrooms_outside_of_centre_pr"),
            item.get("price_per_square_meter_to_buy_apartment_in_city_centre"),
            item.get("price_per_square_meter_to_buy_apartment_in_city_centre_pr"),
            item.get("price_per_square_meter_to_buy_apartment_outside_of_centre"),
            item.get("price_per_square_meter_to_buy_apartment_outside_of_centre_pr"),
            item.get("average_monthly_net_salary_after_tax"),
            item.get("average_monthly_net_salary_after_tax_pr"),
            item.get("mortgage_interest_rate_in_percentages__yearly__for_20_years_fixed_rate"),
            item.get("mortgage_interest_rate_in_percentages__yearly__for_20_years_fixed_rate_pr"),
            item.get("last_update")
        ))
        self.connection.commit()
        return item
