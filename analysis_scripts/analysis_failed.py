import asyncio
import datetime as dt
from pathlib import Path
import sys

#print("dt module:", dt)
#print("dt file:", getattr(dt, "__file__", None))
#print("has dt.timezone:", hasattr(dt, "timezone"))
#print("dt.timezone:", getattr(dt, "timezone", None))
#print("has dt.timezone.utc:", hasattr(getattr(dt, "timezone", None), "utc"))
#print(f"anlys_date: {dt.datetime.now(dt.timezone.utc)}")

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

#print("SCRIPT:", Path(__file__).resolve())
#print("PROJECT_ROOT:", PROJECT_ROOT)
#print("logger.py exists?:", (PROJECT_ROOT / "logger.py").exists())
#print("sys.path[0:3]:", sys.path[:3])
# LOGGER    
from logger import get_logger
logger = get_logger(__name__)

from analysis_scripts import constants
from analysis_scripts.ai_master import AIMaster
from analysis_scripts.ai_model_params import AIModelParams
from analysis_scripts.constants import TableNames
from analysis_scripts.db_neon_pooler import get_db_connection, neon_select, neon_delete, neon_insert_record, neon_update
from analysis_scripts.db_neon_wrapper import Policies,  PoliciesErrors
from analysis_scripts.eval_phase1 import ai_analysis_phase1
from analysis_scripts.eval_phase2 import ai_analysis_phase2
from analysis_scripts.eval_phase3 import ai_analysis_phase3
from analysis_scripts.eval_phase4 import ai_analysis_phase4
from analysis_scripts.eval_phase5 import ai_analysis_phase5
from analysis_scripts.parquetfileio import read_from_parquet, write_to_parquet
from analysis_scripts.policy_analysis_data import PolicyAnalysisData
from analysis_scripts.reference_data import load_ref_data_once,  get_ref_data
from analysis_scripts.translator import Translator
from analysis_scripts.setup import reload_all

# LOCAL CONSTANTS
class AnalysisError:
    def __init__(self):
        try:
            self.neon_db_conn =  get_db_connection()
            if not self.neon_db_conn:
                logger.info("No neon database connection")
            self.ai_master = AIMaster()
            self.ai_model_params = AIModelParams()                       
            self.translator = Translator(self.ai_master)
            if not self.translator:
                logger.info("No translator found")
            self.ref_data = load_ref_data_once()
            if not self.ref_data:
                logger.info("No reference data found")
        except Exception as e:
            logger.error(f"analysis innitialization failed. {e}")

    async def aclose(self):
        await self.ai_master.get_client().close()
    
    #####################################################################
    # EVALUATE
    # Main routine to analyze one story
    #####################################################################
    async def evaluate(self, policy_analysis_data: PolicyAnalysisData):
        logger.info(f"START EVALUATION FOR POLICY {policy_analysis_data.id}")
        try:
            # print(policy_analysis_data.model_dump_json(indent=2))
            
            # Get department
            dept_en = policy_analysis_data.dept_en
            if not dept_en:
                policy_analysis_data.errmsg = "Could not find department code"
                policy_analysis_data.success = False
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                ok = False
                return False
            
            ref_data = get_ref_data()
            dept_id = ref_data.departments.get(dept_en, {}).get("id", 0)
            if not dept_id:
                policy_analysis_data.errmsg = f"Could not find department id for code={dept_en}"
                policy_analysis_data.success = False
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                ok = False
                return False
            
            policy_analysis_data.department_id = dept_id
    
            # DO TRANSLATION
            ok, english_translation, errmsg = await self.translator.translate(text_cn=policy_analysis_data.chinese_original, story_id=policy_analysis_data.id)
    
            if not ok:
                policy_analysis_data.success = False
                policy_analysis_data.errmsg = f"policy_analysis.id={policy_analysis_data.id}: Translation failed "
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                return False
            
            logger.info(f"Translation succeeded policy {policy_analysis_data.id}: {english_translation[:75]}")
            policy_analysis_data.english_translation =  english_translation
            
            # Run AI analysis
            ok = await self.full_ai_analysis(policy_analysis_data)
            if not ok:
                policy_analysis_data.success = False
                policy_analysis_data.errmsg = "AI analysis failed"
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                return False
    
            policy_analysis_data.success = True
            return True
    
        finally:
            if not policy_analysis_data.success:
                logger.error(f"Failure for {policy_analysis_data.id}")

            logger.info(f"END EVALUATION FOR POLICY {policy_analysis_data.id}. Status={ok}")
            
    async def update_databases(self, policy_analysis_data: PolicyAnalysisData):
        try:
            # IF ok is True, save to POLICIES table
            # write_to_jsonl(record=policy_analysis_data, filename_prefix='policy_analyssis_data')
            ok = self.insert_policy(policy_analysis_data)
            if not ok:
                policy_analysis_data.success = False
                policy_analysis_data.errmsg = "Could not insert into database"
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                return False
    
            # if policy_analysis is ok and insert_policy worked, delete from errors database
            ok, delete_count = neon_delete(constants.TableNames.TBL_POLICIES_ERRORS, where_clause='source_url=%s', params=(policy_analysis_data.source_url, ))
            if not ok:
                policy_analysis_data.success = False
                policy_analysis_data.errmsg = "Could not delete from {constants.TableNames.TBL_POLICIES_ERRORS}"
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                return False
            
            policy_analysis_data.success = True
            return True
        except Exception as e:
            logger.error(f"update_databases: e={e}")
            ok = False
            return False
        
        finally:
            logger.info(f"END UPDATED DATABASES FOR POLICY {policy_analysis_data.id}. Status={ok}")
    
    #####################################################################
    # FULL_AI_ANALYSIS
    #####################################################################        
    async def full_ai_analysis(self, policy_analysis_data: PolicyAnalysisData):
        logger.info(f"START AI analysis for policy {policy_analysis_data.id}")
        status = False
        try:
            
            success = await ai_analysis_phase1(ai_master=self.ai_master, ai_model_params=self.ai_model_params, policy_analysis_data=policy_analysis_data)
            if not success:
                logger.error(f"full_ai_analysis: id={policy_analysis_data.id}: Could not complete phase 1")
                return False
            success = await ai_analysis_phase2(ai_master=self.ai_master, ai_model_params=self.ai_model_params, policy_analysis_data=policy_analysis_data)
            if not success:
                logger.error(f"full_ai_analysis: id={policy_analysis_data.id}: Could not complete phase 2")
                return False
            success = await ai_analysis_phase3(ai_master=self.ai_master, ai_model_params=self.ai_model_params, policy_analysis_data=policy_analysis_data)
            if not success:
                logger.error(f"full_ai_analysis: id={policy_analysis_data.id}: Could not complete phase 3")
                return False
            success = await ai_analysis_phase4(ai_master=self.ai_master, ai_model_params=self.ai_model_params, policy_analysis_data=policy_analysis_data)
            if not success:
                logger.error(f"full_ai_analysis: id={policy_analysis_data.id}: Could not complete phase 4")
                return False
            success = await ai_analysis_phase5(ai_master=self.ai_master, ai_model_params=self.ai_model_params, policy_analysis_data=policy_analysis_data)
            if not success:
                logger.error(f"full_ai_analysis: id={policy_analysis_data.id}: Could not complete phase 5")
                return False
            
            status = True
            return True
        except Exception as e:
            logger.error(f"full_ai_analysis. Error encountered for policy {policy_analysis_data.id}: {e}")
            return False
        
        finally:
            logger.info(f"END AI analysis for policy {policy_analysis_data.id}. status={status}")
            
    #####################################################################
    # INSERT_POLICY
    # insert working data into policies table
    #####################################################################    
    def insert_policy(self, policy_analysis_data: PolicyAnalysisData) -> bool:
        try:
            policy: Policies = Policies()
            policy.anlys_date = dt.datetime.now(dt.timezone.utc)
            policy.chinese_original = policy_analysis_data.chinese_original
            policy.continent_tags = policy_analysis_data.continent_tags
            policy.country_tags = policy_analysis_data.country_tags
            policy.country_region_tags = policy_analysis_data.country_region_tags
            policy.department_id =  policy_analysis_data.department_id
            policy.description = policy_analysis_data.description
            policy.title = policy_analysis_data.english_title
            policy.english_translation = policy_analysis_data.english_translation
            policy.impact = policy_analysis_data.importance_score
            policy.impact_analysis = [
                impact.model_dump(exclude_none=True, by_alias=True) if hasattr(impact, "model_dump") else impact
                for impact in (policy_analysis_data.impact_analysis or [])
            ]
            policy.importance_score = policy_analysis_data.importance_score
            policy.industry_ICB_tags = policy_analysis_data.industry_ICB_tags
            policy.province_tags = policy_analysis_data.province_tags
            policy.region_tags = policy_analysis_data.region_tags
            policy.source_url = policy_analysis_data.source_url
            if isinstance(policy_analysis_data.time, str):
                policy.time = policy_analysis_data.time
            elif isinstance(policy_analysis_data.time, dt.datetime):
                policy.time = policy_analysis_data.time.strftime("%Y-%m-%d")
            else:
                raise TypeError(f"Unsupported type for time: {type(time)}")            
            
            # FULL CONTENT
            key_points = policy_analysis_data.key_points or []
            policy.full_content = {
                "key_points": [
                    kp.model_dump(exclude_none=True, by_alias=True) if hasattr(kp, "model_dump") else kp
                    for kp in key_points
                    ],
                "analysis": policy_analysis_data.comprehensive_analysis,
            }
            
            # TOPICS TAGS
            policy.tags = {}
            if policy_analysis_data.topics_tags:
                policy.tags["topics"] = policy_analysis_data.topics_tags
            policy.status = 1
            
            # SAVE TO OUTPUT FILE
            # write_to_jsonl(policy,  filename_prefix='policy_new')
            # SAVE RECORD
            policy.status = 1
            ok = neon_insert_record(
                table_name=constants.TableNames.TBL_POLICIES,
                data=policy,
                exclude_list=['id']
            )        
            return ok
        except Exception as e:
            logger.error(f"insert_neondb: Error encountered. {e}")
            return False
        
async def process_records(newset):
    """Process all records within a single event loop"""
    for idx, story in enumerate(newset, 1):
        analysis: AnalysisError = AnalysisError()
        
        try:
            policy_analysis_data: PolicyAnalysisData = PolicyAnalysisData()
            policy_analysis_data.chinese_original = story.chinese_original
            policy_analysis_data.dept_en = story.dept_en
            policy_analysis_data.source_url = story.source_url
            policy_analysis_data.title_cn = story.title_cn
            policy_analysis_data.time = story.time
            policy_analysis_data.id = idx
            
            ok = await analysis.evaluate(policy_analysis_data)
            if ok:
                ok = await analysis.update_databases(policy_analysis_data)
            else:
                data: dict = {'status': -1}
                neon_update(constants.TableNames.TBL_POLICIES_ERRORS, data=data, where_clause='source_url=%s', params=(policy_analysis_data.source_url, ))
        
        finally:
            await analysis.aclose()
            logger.info(f"Record {idx} connections closed")

def correct_errors():
    # GET EVERYTHING FROM TABLE STATUS = 0
    ok, result = neon_select(constants.TableNames.TBL_POLICIES_ERRORS, where_clause="status=%s", params=(0, ))
    if not ok:
        logger.error("could not read from table")
        return False    
    models: List[PoliciesErrors] = [PoliciesErrors(**record) for record in result]
    logger.info(f"{constants.TableNames.TBL_POLICIES_ERRORS} contains {len(models)} records")
    
    # VERIFY IF IT IS ALREADY IN THE FINAL POLICIES TABLE
    # IF IT IS, REMOVE FROM THE ERRORS TABLE AND THE MODELS LIST
    # REMOVE ANY RECORDS THAT HAVE ALREADY BEEN PROCESSED
    newset = []
    for idx, record in enumerate(models, 1):
        ok, result = neon_select(constants.TableNames.TBL_POLICIES, where_clause='source_url = %s', params=(record.source_url,))
        if not ok:
            logger.error("could not read from table")
            return False
        if not result:
            newset.append(record)
        else:
            ok, count = neon_delete(constants.TableNames.TBL_POLICIES_ERRORS, where_clause='source_url = %s', params=(record.source_url,))
            if not ok:
                logger.error(f"could not delete from table {constants.TableNames.TBL_POLICIES_ERRORS}")
                return False            
        #if len(newset) > 1:
        #    break
        if idx % 20 == 0:
            logger.info(f"{idx} of {len(models)} stories")
    
    logger.info(f"correct_errors: final list contains {len(newset)} records")
    asyncio.run(process_records(newset))
            
    ok, result = neon_select(constants.TableNames.TBL_POLICIES_ERRORS)
    if not ok:
        return True
    logger.info(f"{constants.TableNames.TBL_POLICIES_ERRORS} contains {len(result)} records")
    return True
            
def test_all():
    #ok, records = neon_select(constants.TableNames.TBL_POLICIES_ERRORS)
    #if not ok:
    #    exit(0)
    #ok = write_to_parquet(records=records, output_dir="", filename_prefix="erase")
    
    # GET FAILED RECORD DATA
    ok, records = read_from_parquet(output_dir="", filename_prefix="erase")
    if not ok:
        logger.error("Could not read from data file")
        return False
    
    # CONVERT TO PoliciesErrors
    models: List[PoliciesErrors] = [PoliciesErrors(**record) for record in records]
    
    # REMOVE ANY RECORDS THAT HAVE ALREADY BEEN PROCESSED
    newset = []
    for idx, record in enumerate(models, 1):
        ok, result = neon_select(constants.PROD_TBL_POLICIES, where_clause='source_url = %s', params=(record.source_url,))
        if not ok:
            logger.error("could not read from table")
            return False
        if not result:
            newset.append(record)
        if idx > 1:
            break
    
    # LOOP THROUGH FAILED RECORDS AND INSERT ANY INTO THE TEST TABLE THAT AREN'T ALREADY THERE
    for idx, record in enumerate(newset, 1):
        ok, result = neon_select(constants.TableNames.TBL_POLICIES_ERRORS, where_clause='source_url = %s', params=(record.source_url,))
        if not ok:
            logger.error("could not read from table")
            return False
        if not result and result[0]:
            record_dict = record[0].model_dump(exclude={'id'})
            ok = neon_insert_record(table_name=constants.TableNames.TBL_POLICIES_ERRORS, data=record_dict)
            if not ok:
                logger.error("Insert failed")
                return False
    
    correct_errors()
    
if __name__ == "__main__":
    # test1()
    # asyncio.run(test2())
    # asyncio.run(test3())
    # test4()
    correct_errors()
    # test_error()