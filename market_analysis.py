import numpy as np
# input example index='2021-08'
def calculate_indicators(index):
    listings = hook.execute_raw_query(
    query=f"""
    select count(distinct l.dw_property_id) as availability,l.num_of_bedrooms as room,r.region,r.zone,
    cast(p.completion_year>={index[:4]} as INT) as presale,s.school_name,mr.transportation_name,'{index}' as time_index from datamart.view_listing l 
    left join datamart.view_property m using(dw_property_id)
    left join datamart.view_address_expand r using(dw_address_id)
    left join datamart.view_project_expand p using(dw_project_id)
    left join datamart.check_school_to_property_nearby_sg s using(dw_property_id)
    left join datamart.check_mrt_to_property_nearby_sg mr using(dw_property_id)
    where LEFT(l.listing_firstseen,4) >='2021'
    and l.listing_type ='sale'
    and LEFT(l.listing_firstseen,7)<= '{index}'
    and LEFT(l.listing_lastseen,7)>='{index}'
    and r.country='sg'
    and m.property_type_group ='Condo'
    group by room,r.region,r.zone,presale,s.school_name,mr.transportation_name,time_index""",
    output_format=OutputFormat.pandas
    )
    transactions= hook.execute_raw_query(
                    query = f"""
                    select count(distinct(l.dw_transaction_id)) as transactions,cast(p.completion_year>={index[:4]} as INT) as new_transacted,a.region,a.zone,
                    h.num_of_bedrooms as room,s.school_name,mr.transportation_name,'{index}' as time_index from datamart.view_transaction_expand l
                    left join datamart.view_address_expand a using(dw_address_id) left join datamart.view_project_expand p 
                    using(dw_project_id) left join datamart.view_property h using(dw_property_id)
                    left join datamart.check_school_to_property_nearby_sg s using(dw_property_id)
                    left join datamart.check_mrt_to_property_nearby_sg mr using(dw_property_id)
                    where l.transaction_type ='sale'
                    and LEFT(l.transaction_date,7)='{index}'
                    and l.country='sg'
                    and l.data_origin ='sg_ura_sale'
                    and l.property_type_group ='Condo'
                    group by new_transacted,room,a.region,a.zone,s.school_name,mr.transportation_name,time_index
                    """,
                    output_format=OutputFormat.pandas)
    stock=hook.execute_raw_query(
                        query = f"""
                        select sum(a.num_of_units) as stock,a.zone,a.region,a.school_name,a.transportation_name,
                        a.room,a.time_index  from
                        (select distinct(l.dw_project_id),l.num_of_units,m.zone,m.region,s.school_name,
                        mr.transportation_name,r.num_of_bedrooms as room,'{index}' as time_index
                        from datamart.view_project_expand l
                        left join datamart.view_address_expand m using(dw_address_id)
                        left join datamart.view_property_expand r using (dw_project_id)
                        left join datamart.check_school_to_property_nearby_sg s using(dw_property_id)
                        left join datamart.check_mrt_to_property_nearby_sg mr using(dw_property_id)
                        where r.property_type_group ='Condo'
                        and m.country='sg'
                        and l.completion_year<{index[:4]}
                        ) as a 
                        group by a.zone,a.region,a.school_name,a.transportation_name,a.room,a.time_index
                        """,
                        output_format=OutputFormat.pandas)
    joined = listings.merge(transactions,on=['zone','region','school_name',\
                                         'transportation_name','room','time_index'],how='left')
    joined=joined.merge(stock,on=['zone','region','school_name',\
                                         'transportation_name','room','time_index'],how='left')
    joined= joined.fillna(value=np.nan)
    hook.copy_from_df(
                df=joined,
                target_table="datamart.market_analysis_sg",
                mode=DBCopyMode.APPEND
            )