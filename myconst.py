CIAN_LOAD_SQL_BKP = '''
            select 	aa.src_cian_id
                    ,aa2.region_name 
                    ,daca.advert_category_code
                    ,daca.advert_category_name
                    ,aa.realty_latitude
                    ,aa.realty_longitude 
                    ,aa.housing_complex_code 
                    ,aa.housing_complex_name 
                    ,aa.housing_complex_block_code
                    ,aa.housing_complex_block_name
                    ,hcdc_q.commissioning_date
                    ,dbca.building_class_type
                    ,dbca.building_class_name 
                    ,room_cnt
                    ,aa.currency_name
                    ,aa.price_sqm_amt  
           from dl_cian.advert_actual_bkp_20220815 aa 
                    inner join dl_cian.dct_advert_category_actual_bkp_20220815 daca on aa.advert_category_bkh = daca.bkh
                    inner join dl_cian.dct_status_actual_bkp_20220815 dsa on aa.status_bkh = dsa.bkh
                    inner join dl_cian.address_actual_bkp_20220815 aa2 on aa.src_realty_id = aa2.src_realty_id 
                    inner join (select 	housing_complex_block_code,
                                        case 
                                            when commissioning_fact_date is not null
                                                then commissioning_fact_date
                                            else commissioning_plan_date
                                        end commissioning_date 
                                        ,building_class_bkh
                                from dl_cian.dct_housing_complex_block_actual_bkp_20220815 dhcba 
                                where commissioning_plan_date is not null
                                ) hcdc_q on aa.housing_complex_block_code = hcdc_q.housing_complex_block_code
                    inner join dl_cian.dct_building_class_actual_bkp_20220815 dbca on hcdc_q.building_class_bkh = dbca.bkh
            where
                aa.currency_name like 'rur' 
                and daca.advert_category_code in (1,3) 
                and hcdc_q.commissioning_date <= '2022-09-01'::date
                and hcdc_q.commissioning_date >= '2017-09-01'::date
                and dsa.status_code = 1
                and aa2.region_name like 'Москва'
            order by aa.src_cian_id 
        '''

CIAN_LOAD_SQL = '''
            select 	aa.src_cian_id
                    ,aa2.region_name 
                    ,daca.advert_category_code
                    ,daca.advert_category_name
                    ,aa.realty_latitude
                    ,aa.realty_longitude 
                    ,aa.housing_complex_code 
                    ,aa.housing_complex_name 
                    ,aa.housing_complex_block_code
                    ,aa.housing_complex_block_name
                    ,hcdc_q.commissioning_date
                    ,dbca.building_class_type
                    ,dbca.building_class_name 
                    ,room_cnt
                    ,aa.currency_name
                    ,aa.price_sqm_amt  
            from dl_cian.advert_actual aa 
                inner join dl_cian.dct_advert_category_actual daca on aa.advert_category_bkh = daca.bkh
                inner join dl_cian.dct_status_actual dsa on aa.status_bkh = dsa.bkh
                inner join dl_cian.address_actual aa2 on aa.src_realty_id = aa2.src_realty_id 
                inner join (select 	housing_complex_block_code,
                                    case 
                                        when commissioning_fact_date is not null
                                            then commissioning_fact_date
                                        else commissioning_plan_date
                                    end commissioning_date 
                                    ,building_class_bkh
                            from dl_cian.dct_housing_complex_block_actual dhcba 
                            where commissioning_plan_date is not null
                            ) hcdc_q on aa.housing_complex_block_code = hcdc_q.housing_complex_block_code
                inner join dl_cian.dct_building_class_actual dbca on hcdc_q.building_class_bkh = dbca.bkh
            where
                aa.currency_name like 'rur' 
                and daca.advert_category_code in (1,3) 
                and hcdc_q.commissioning_date <= '2022-09-01'::date
                and hcdc_q.commissioning_date >= '2017-09-01'::date
                and dsa.status_code = 1
                and aa2.region_name like 'Москва'
            order by aa.src_cian_id 
        '''


POLYGONS_SQL = '''
            select *
            from dict.quarter_coords
        '''

OBJ_INFO_SQL = '''
            select 	distinct  ao.obj_id 
                    , ao.obj_addr adress 
                    , replace(rf.region_short_desc ,'Город ','') region_name
                    , concat_ws(':', aop.obj_parcel_1, aop.obj_parcel_2, aop.obj_parcel_3) as quarter_cad_numb
                    , ao.obj_lk_latitude realty_latitude
                    , ao.obj_lk_longitude realty_longitude
                    , ao.obj_pool_nm housing_complex
                    , ao.obj_comiss_dt 
                    , ao.obj_lk_class_cd building_class_type
                    , ao.obj_lk_class_desc building_class_name
            from emarti.act_obj ao 
                inner join dict.region_fias rf  on ao.rpd_region_cd  = rf.region_cd  
    left join emarti.act_obj_parcel aop on aop.obj_id = ao.obj_id
            where ao.obj_id = {0} 
'''

CURRENCY_SQL = '''
            select 
                dca.currency_iso_num 
                , dca.currency_iso_code 
                , avg(cea.exchange_rate) as avg_exchange_rate
            from dl_cbr.currency_exchange_actual cea 
            left join dl_cbr.dct_currency_actual dca on dca.bkh = cea.currency_bkh 
            where dca.currency_iso_code in ('USD', 'EUR')
            and cea.exchange_date >= (select date_trunc('month', current_date) - interval '1 day')
            group by 1,2
        '''

OBJ_PRICE_DYNAMICS_SQL = '''
            select  obj_id
                    , report_month_dt
                    , sum(price_conclude_amt)/sum(area_conclude_sq) avg_price_sqm
                    , sum(contract_conclude_cnt) contract_conclude_cnt 
            from emarti.act_obj_concluded_contract  aocc
            where contract_elem_type_cd = 1  
                    and obj_id = {0} 
                    and area_conclude_sq  != 0
            group by 1,2
            order by report_month_dt
        '''

DICT_CSL_CODE_EISGS2CIAN = {
    1: 1,
    2: 4,
    3: 2,
    4: 3
}
OBJ_KEY_FIELDS = ['region_name', 'district', 'subdistrict',
                  'building_class_type', 'building_class_name']
OBJ_INFO_FIELDS = OBJ_KEY_FIELDS + \
    ['adress', 'housing_complex', 'obj_comiss_dt']


INFL_SQL = '''
            select * from dict.infl_rates
            where rpd_region_cd=77
        '''

COMMISSIONING_EFFECT_SQL = '''
            select * from dict.invest_obj_commissioning
            where rpd_region_cd= 77
        '''
COMMISSIONING_DATE_SQL = '''
            select obj_comiss_dt
            from emarti.act_obj
            where obj_id = {0}
        '''

ALL_CIAN_MARKET_DATA_SQL = '''
select 	
		aa.src_cian_id
        ,aa2.region_name
        ,daca.advert_category_code
        ,daca.advert_category_name
        ,aa.realty_latitude
        ,aa.realty_longitude 
        ,aa.housing_complex_code 
        ,aa.housing_complex_name 
        ,aa.housing_complex_block_code
        ,aa.housing_complex_block_name
        ,dbca.building_class_type
        ,dbca.building_class_name 
        ,room_cnt
		,aa.price_sqm_amt
		,drta.repair_type_name
		,drta.repair_type_code
from dl_cian.advert_actual_bkp_20220815 aa 
    inner join dl_cian.dct_advert_category_actual_bkp_20220815 daca on aa.advert_category_bkh = daca.bkh
    inner join dl_cian.dct_status_actual_bkp_20220815 dsa on aa.status_bkh = dsa.bkh
    inner join dl_cian.address_actual_bkp_20220815 aa2 on aa.src_realty_id = aa2.src_realty_id 
    inner join (select 	housing_complex_block_code,
                        case 
                            when commissioning_fact_date is not null
                                then commissioning_fact_date
                            else commissioning_plan_date
                        end commissioning_date 
                        ,building_class_bkh
                from dl_cian.dct_housing_complex_block_actual_bkp_20220815 dhcba 
                where commissioning_plan_date is not null
                ) hcdc_q on aa.housing_complex_block_code = hcdc_q.housing_complex_block_code
    inner join dl_cian.dct_building_class_actual_bkp_20220815 dbca on hcdc_q.building_class_bkh = dbca.bkh
    left join dl_cian.dct_repair_type_actual_bkp_20220815 drta on aa.repair_type_bkh = drta.bkh
where
    aa.currency_name like 'rur' 
    and daca.advert_category_code in (1,3) 
    and hcdc_q.commissioning_date <= '2022-07-01'::date
    and hcdc_q.commissioning_date >= '2017-09-01'::date
    and dsa.status_code = 1
    and aa2.region_name like 'Москва'
    and aa.room_cnt is not null
order by 1
'''

EISGS_MARKET_DATA_SQL = '''
select 	ao.obj_id 
			, rd.city_nm region_name
	        , concat_ws(':', aop.obj_parcel_1, aop.obj_parcel_2, aop.obj_parcel_3) as quarter_cad_numb
	        , ao.obj_lk_latitude realty_latitude
	        , ao.obj_lk_longitude realty_longitude
	        , ao.obj_lk_class_cd building_class_type
	        , ao.obj_lk_class_desc building_class_name
	        , ao.obj_comiss_dt 
	        , aocc.report_month_dt
	        , (ao.obj_comiss_dt  - aocc.report_month_dt)/30 report_before_commis_months
			, sum(aocc.price_conclude_amt)/sum(aocc.area_conclude_sq) price_sqm_amt
			, sum(aocc.contract_conclude_cnt) contract_conclude_cnt 
	from emarti.act_obj ao 
		inner join emarti.act_obj_concluded_contract  aocc on ao.obj_id = aocc.obj_id 
		inner join dict.region_districts rd on ao.rpd_region_cd  = rd.rpd_region_cd 
	    left join emarti.act_obj_parcel aop on aop.obj_id = ao.obj_id
	where rd.rpd_region_cd = 77 and area_conclude_sq  != 0 
			 and (aocc.obj_id,aocc.report_month_dt) in (select obj_id ,max( report_month_dt)       
											from emarti.act_obj_concluded_contract aocc 
											group by 1 )
	group by 1,2,3,4,5,6,7,8,9
'''

DCT_BUILDING_CLS_SQL = '''
select building_class_type ,
		building_class_name 
from dl_cian.dct_building_class_actual dbcab 
'''

BUILDING_CLS_REL_DCT = {'бизнес': {'бизнес': 1.0,
                                   'комфорт': 0.7644110275689223,
                                   'премиум': 1.4897082635578043,
                                   'эконом': 0.700015},
                        'комфорт': {'бизнес': 1.3081967213114754,
                                    'комфорт': 1.0,
                                    'премиум': 1.8582298893495808,
                                    'эконом': 0.9337426136061482},
                        'премиум': {'бизнес': 0.6715010377086479,
                                    'комфорт': 0.5383478554235742,
                                    'премиум': 1.0,
                                    'эконом': 0.5406067329148853},
                        'эконом': {'бизнес': 1.4285408169824934,
                                   'комфорт': 1.0709786184829664,
                                   'премиум': 1.849852713759721,
                                   'эконом': 1.0}}



