from enum import Enum


PROJECT = 'dynamic-sun-383413'

class DATASET(Enum): 
    all_sites = 'analytics_368525906'
    ca = 'analytics_331311936'
    be = 'analytics_320569573'
    fr = 'analytics_331245319'
    ch = 'analytics_331061376'
    mx = 'analytics_331320912'
    pt = 'analytics_331504061'
    typeform = 'typeform'

EVENT_PARAM_TYPES = ['string', 'int', 'float', 'double']