import re
import geocoder
import pandas as pd
import nltk
import os

from metaphone import doublemetaphone
from unidecode import unidecode

dir_path = os.path.dirname(os.path.realpath(__file__))
CAR_gazetteer_file = os.path.join(dir_path,'CAR_gazetteer.xls')
CAR_gazetteer = pd.read_excel(CAR_gazetteer_file, sheet_name='CAR_gazetteer')

#geonames - type code dictionary:
geonames_code_dict = {"PCLI":0,"ADM1":1,"ADM2":2,"ADM3":3,"PPL":3,"PPLL":3,"PPLA":1,"PPLC":1,"PPLA2":2,"PPLA3":3}

def loc_finder(loc):
    
    #preprocessing: clean white spaces, diacritics and lowercase words
    CAR_alias = ['CARcrisis','Centrafrique',"Central African Republic","CAR","RCA","C.Africa"]
    Bangui_alias = ['Bangui',"Pk5","PK5","km5","pk5","KM5","PK 26","pk3","PK12","km 12","PK12","M'Poko","MPoko"]
    if loc in CAR_alias:
        loc = "Central African Republic"
    elif loc in Bangui_alias:
        loc = "Bangui"
        
    loc = re.sub("^\s+","",loc)
    loc = unidecode(loc)
    #replace with regex
    loc = loc.replace("N'","N")
    loc = loc.replace("M'","M")
    loc = loc.replace("G'","G")
    loc = re.sub(r'(?<!-)\b[a-z]+\s*', "", loc).strip()
    #code to eliminate silent G. Enable as needed
    loc = loc.replace("Gb","B")
    loc = loc.replace("gb","b")
    
    #expert knowledge phase
    if loc == "DRC":
        loc = "DR Congo"
    elif loc == "Burkina":
        loc = "Burkina Faso"
    elif loc == "Beni":
        loc = "Beni Kivu"
    elif loc == "Congo":
        loc = "Congo Republic"
    elif loc == "France":
        return None
    elif loc == "Diabaly":
        loc = "Diabali"
    elif loc == "":
        return None
    
    print(loc+" is the loc fed into the geocoders")
    #1st phase: country
    geocode = geocoder.geonames(loc,key='oraculo_av',
                            fuzzy=1.0, 
                            maxRows = 1,
                            #featureClass=('P','A'))
                            featureCode='PCLI')
                            #east=39.5,
                            #west=-9.5,
                            #north=25.5,
                            #south=-5.6,
    if not not geocode:
        #return geocode object later
        if nltk.edit_distance(loc, geocode.raw["name"])<=2:
            print(geocode.raw["name"]+ " was the geocoder result (0th pass)")
            return geocode
    
    #2nd phase: city/region in Africa. THIS IS THE MOST UP_TO_DATE FUNCTION (22 DEC 2020)
    geocode = geocoder.geonames(loc, 
                                fuzzy=0.95, 
                                countryBias="cf", 
                                continentCode='AF', 
                                key="oraculo_av", 
                                maxRows = 1,
                                featureCode=('PPL','PPLA','PPLA2','PPLA3','PPLA4','PPLA5','PPLC','PPLCH','PPLF','PPLG','PPLH','PPLL','PPLQ','PPLR','PPLS','PPLW','PPLX','ADM1'))
                                #east=39.5,
                                #west=-9.5,
                                #north=25.5,
                                #south=-5.6)
    if not not geocode:
        #return geocode object later
        if loc == "Beni Kivu":
            loc = "Beni"
        
        clean_name = geocode.raw["name"]
        clean_name = clean_name.replace("Province du ","")
        clean_name = clean_name.replace(" Zone","")
    
        if nltk.edit_distance(loc, clean_name)<=3 or nltk.edit_distance(loc, geocode.raw["adminName1"])<=2:
            print(geocode.raw["name"]+ " was the geocoder result (first pass)")
            return geocode
    
    #3rd phase: CAR gazetteer
    edit_dist_df_data = []
    for PName in pd.Series(CAR_gazetteer["PName1"].tolist()):
        edit_dist_df_data.append([PName,nltk.edit_distance(loc, PName)])

    edit_dist_df = pd.DataFrame(edit_dist_df_data, columns=["PName1","edit_dist"])
    edit_dist_df = edit_dist_df.sort_values("edit_dist",ascending=True)
    closest_loc = edit_dist_df.loc[edit_dist_df["edit_dist"]<=1].copy()
    #print(closest_loc)
    metaloc = doublemetaphone(loc)[0]
    loc_N = "N"+loc
    metaloc_N = doublemetaphone(loc_N)[0]
    metaloc_list = []
    for loc3 in closest_loc.PName1.tolist():
        metaloc_list.append(doublemetaphone(loc3)[0])
    #print(metaloc_list)
    closest_loc["metaphone"] = metaloc_list
    #print(closest_loc)
    try:
        closest_loc = closest_loc.loc[(closest_loc['metaphone'] == metaloc)|\
                                      (closest_loc['metaphone'] == metaloc_N)]
        #print(closest_loc)
        if len(closest_loc) != 0:
            closest_loc = closest_loc.PName1.iloc[0]
            loc_option = CAR_gazetteer.loc[CAR_gazetteer["PName1"] == closest_loc]
            print(loc_option.PName1.iloc[0] + " was the gazetteer result")
            return loc_option
    
    except:
        pass
    
    #GeoNames geocoder phase
    #geocode = RateLimiter(geolocator.geocode, min_delay_seconds=5)
    #geocode = geocoder.geonames(loc, fuzzy=0.8, key="oraculo_av", countryBias="cf", maxRows = 6)
    #counter = 0
    #while counter < 2:
    geocode = geocoder.geonames(loc, 
                                fuzzy=0.75, 
                                key="oraculo_av", 
                                countryBias="cf", 
                                continentCode='AF',
                                featureClass=('P','A'),
                                maxRows = 3, 
                                east=39.5,
                                west=-9.5,
                                north=25.5,
                                south=-5.6)

    if not not geocode:
        #return geocode object later
        for hyp_loc in geocode:
            print(hyp_loc.raw['name'])
            if (nltk.edit_distance(loc, hyp_loc.raw["name"])<=3) and (doublemetaphone(hyp_loc.raw['name'])[0] == metaloc): 
                print(hyp_loc.raw["name"]+ " was the geocoder result")
                return hyp_loc
            else:
                continue
    print("error/no plausible African toponyms were found for loc "+loc)
    return None

def loc_selector(loc_list):
    
    #print(loc_list)
    decision_table_data = []
    likely_country = ""
    likely_region = ""
    
    for loc in loc_list:
        
        out = loc_finder(loc)
        
        if type(out) == pd.core.frame.DataFrame:
            for i in range(len(out)):
                out_name = out.PName1.iloc[i]
                out_region = out.ADM1_NAME.iloc[i]
                out_country = "Central African Republic"
                out_long = out.X_longitud.iloc[i]
                out_lat = out.Y_Latitude.iloc[i]
                out_type = out.TypeCode.iloc[i]
                
                out_data = [out_name,out_region,out_country,out_lat,out_long,out_type]
                decision_table_data.append(out_data)
        else:
            if isinstance(out, str) == True:
                continue
            elif out == None:
                continue
            elif not not out:
                try:
                    for loc_hyp in out:
                        out_name = loc_hyp.raw["name"]
                        out_region = (unidecode(loc_hyp.raw["adminName1"])).replace("-"," ")
                        out_lat = loc_hyp.raw["lat"]
                        out_long = loc_hyp.raw["lng"]
                        try:
                            out_country = loc_hyp.raw["countryName"]
                        except:
                            out_country = loc_hyp.raw["name"]
                        try:
                            out_type = geonames_code_dict[loc_hyp.raw["fcode"]]
                        except:
                            out_type = 0

                        out_data = [out_name,out_region,out_country,out_lat,out_long,out_type]
                        decision_table_data.append(out_data)
                except:
                    out_name = out.raw["name"]
                    out_region = (unidecode(out.raw["adminName1"])).replace("-"," ")
                    out_lat = out.raw["lat"]
                    out_long = out.raw["lng"]
                    try:
                        out_country = out.raw["countryName"]
                    except:
                        out_country = out.raw["name"]
                    try:
                        out_type = geonames_code_dict[out.raw["fcode"]]
                    except:
                        out_type = 0

                    out_data = [out_name,out_region,out_country,out_lat,out_long,out_type]
                    decision_table_data.append(out_data)
            else:
                loc_hyp = out
                out_name = loc_hyp.raw["name"]
                out_region = (unidecode(loc_hyp.raw["adminName1"])).replace("-"," ")
                out_lat = loc_hyp.raw["lat"]
                out_lat = loc_hyp.raw["lng"]
                try:
                        out_country = loc_hyp.raw["countryName"]
                except:
                        out_country = loc_hyp.raw["name"]
                try:
                    out_type = geonames_code_dict[loc_hyp.raw["fcode"]]
                except:
                    out_type = 0


                out_data = [out_name,out_region,out_country,out_lat,out_long,out_type]
                decision_table_data.append(out_data)
            
            if out_country == "Central African Republic":
                likely_country = "Central African Republic"
                
            #if loc_hyp.raw["fcode"] == "ADM1":
                #likely_region = out_region
                #print(likely_region)
    
    decision_df = pd.DataFrame(decision_table_data, columns=["Name","Adm1","Country","Lat","Long","Type"])
    print(decision_df)
    
    if likely_country == "":
            try:
                if decision_df["Country"].value_counts()[0]>1:
                    likely_country = decision_df["Country"].value_counts().idxmax()
                else:
                    likely_country = ""
            except:
                likely_country = ""
                
    if likely_region == "":
        try:
            if decision_df["Adm1"].value_counts()[0]>1: 
                likely_region = decision_df["Adm1"].value_counts().idxmax()
            else:
                likely_region = ""
        except:
            likely_region = ""

    try:
        if likely_country != "" and likely_region != "":
            spec_loc_df = decision_df.loc[(decision_df.Country == likely_country)&(decision_df.Adm1 == likely_region)]
        elif likely_country == "" and likely_region != "":
            spec_loc_df = decision_df.loc[decision_df.Adm1 == likely_region]
        elif likely_country != "" and likely_region == "":
            spec_loc_df = decision_df.loc[(decision_df.Country == likely_country)]
        else:
            spec_loc_df = decision_df
        print(spec_loc_df)
        final_loc = spec_loc_df.loc[spec_loc_df.Type == spec_loc_df.Type.max()].head(1)
        #final_loc = decision_df.loc[decision_df.Type == decision_df.Type.max()].head(1)
        return final_loc
    except:
        return None
    

