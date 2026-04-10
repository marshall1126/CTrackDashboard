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
from analysis_scripts.eval_phase1 import ai_analysis_phase1
from analysis_scripts.eval_phase2 import ai_analysis_phase2
from analysis_scripts.eval_phase3 import ai_analysis_phase3
from analysis_scripts.eval_phase4 import ai_analysis_phase4
from analysis_scripts.eval_phase5 import ai_analysis_phase5
from analysis_scripts.jsonfileio import read_from_jsonl, write_to_jsonl
from analysis_scripts.models import PolicyAnalysisData
from analysis_scripts.reference_data import load_ref_data_once,  get_ref_data
from analysis_scripts.translator import Translator
from analysis_scripts.setup import reload_all
from analysis_scripts.database.base_database import BaseDatabaseManager
from analysis_scripts.database.neon_manager import NeonConnectionMode, NeonManager
from analysis_scripts.models import Policies, PoliciesErrors, StoryAllFinal, AnalysisLastUpdt

# LOCAL CONSTANTS
class Analysis:
    def __init__(self):
        try:
            self.db_manager: BaseDatabaseManager = NeonManager(NeonConnectionMode.POOLER)
            if not self.db_manager:
                logger.info("No database connection")
            ok = self.db_manager.db_connect()
            if not ok:
                logger.info("No database connection")            
            self.ai_master = AIMaster()
            self.ai_model_params = AIModelParams()                       
            self.translator = Translator(self.ai_master)
            if not self.translator:
                logger.info("No translator found")
            ref_data = load_ref_data_once(self.db_manager)
            if not ref_data:
                logger.error("No reference data found")
            self.policy_count = 0
        except Exception as e:
            logger.error(f"analysis innitialization failed. {e}")

    async def aclose(self):
        await self.ai_master.get_client().close()
    
    #####################################################################
    # PREPROCESS
    #####################################################################        
    def preprocess(self):
        logger.info ("preprocess: entered")
        
        try:
            # read the final stories table
            status, stories = self.db_manager.db_select(table_name=constants.TableNames.SCRAPE_STORIES_ALL_FINAL, limit=1, dataclass=StoryAllFinal)
            if not status:
                logger.info (f"preprocess: Error {constants.TableNames.SCRAPE_STORIES_ALL_FINAL}")
                return False, []
            if not stories:
                logger.info ("preprocess: No stories found")
                return True, []            
            
            # eliminate any story where success flag is false
            # Keep only successful stories
            successful_stories = [s for s in stories if s.success]
            
            # Remove stories with duplicate links
            seen_links = set()
            unique_stories = []
            
            for s in successful_stories:
                link = s.link
                if link in seen_links:
                    continue
                seen_links.add(link)
                unique_stories.append(s)            
        
            removed_count = len(stories) - len(unique_stories)
            logger.info(f"preprocess: {len(unique_stories)} good stories, {removed_count} filtered stories")
            
            # eliminate rows where duplicates exist
            new_story_list = []
            for idx, story in enumerate(unique_stories, 1):
                source_url = story.link
                
                ok, found1 = self.db_manager.db_select(
                    constants.TableNames.TBL_POLICIES_ERRORS,
                    where={"source_url": source_url}
                )
                if not ok: # System error
                    return False, []
                # ignore and continue if found in errors table
                if found1:
                    continue
                # look in the POLICIES TABLE using the same where condition
                ok, found2 = self.db_manager.db_select(
                    constants.TableNames.TBL_POLICIES,
                    where={"source_url": source_url}
                )
                    
                if not ok: # System error
                    return False, []
                # ignore and continue if found in policies table
                if found2:
                    continue
                new_story_list.append(story)
                
            logger.info (f"preprocess: {len(new_story_list)} stories found")
            
            # DELETE THEM FROM THE SUPABASE TABLE
            
            return True, new_story_list
        except Exception as e:
            logger.error(f"preprocess: Error enncountered. {e}")
            return False, []

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
                return False
            
            ref_data = get_ref_data()
            dept_id = ref_data.departments.get(dept_en, {}).get("id", 0)
            if not dept_id:
                policy_analysis_data.errmsg = f"Could not find department id for code={dept_en}"
                policy_analysis_data.success = False
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
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
    
            # IF ok is True, save to POLICIES table
            # write_to_jsonl(record=policy_analysis_data, filename_prefix='policy_analyssis_data')
            ok = self.insert_policy(policy_analysis_data)
            if not ok:
                policy_analysis_data.success = False
                policy_analysis_data.errmsg = "Could not insert into database"
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                return False
    
            # if policy_analysis is ok and insert_policy worked, delete from database
            ok, _ = self.db_manager.db_delete(constants.TableNames.SCRAPE_STORIES_ALL_FINAL, where={StoryAllFinal.FIELD_LINK: policy_analysis_data.source_url})
            if not ok:
                policy_analysis_data.success = False
                policy_analysis_data.errmsg = "Could not delete from database"
                logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
                return False
            logger.info(f"Removed story from {constants.TableNames.SCRAPE_STORIES_ALL_FINAL}")
            policy_analysis_data.success = True
            return True
    
        finally:
            if not policy_analysis_data.success:
                logger.error(f"Failure for {policy_analysis_data.id}")
                # Add to error table
                ok = self.insert_policy_error(policy_analysis_data)
                if ok: # if successful, delete from table
                    ok, _ = self.db_manager.db_delete(constants.TableNames.SCRAPE_STORIES_ALL_FINAL, where={StoryAllFinal.FIELD_LINK: policy_analysis_data.source_url})
                    if not ok:
                        policy_analysis_data.errmsg = "Could not delete from database"
                        logger.error(f"evaluate policy {policy_analysis_data.id}: {policy_analysis_data.errmsg}")
            else:            
                ok = True
            logger.info(f"END EVALUATION FOR POLICY {policy_analysis_data.id}. Status={ok}")
    
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
                policy.time = policy_analysis_data.time
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
            ok = self.db_manager.db_insert(
                table_name=constants.TableNames.TBL_POLICIES,
                data=policy,
                exclude_list=['id']
            )
            self.policy_count += 1
            return ok
        except Exception as e:
            logger.error(f"insert_neondb: Error encountered. {e}")
            return False

    #####################################################################
    # INSERT_POLICY_ERROR
    # Insert outline of policy to error table
    #####################################################################    
    def insert_policy_error(self, policy_analysis_data: PolicyAnalysisData) -> bool:
        try:
            policy_error: PoliciesErrors = PoliciesErrors()
            
            policy_error.chinese_original = policy_analysis_data.chinese_original
            policy_error.dept_en = policy_analysis_data.dept_en
            policy_error.errmsg = policy_analysis_data.errmsg
            policy_error.source_url =  policy_analysis_data.source_url
            policy_error.time = policy_analysis_data.time
            policy_error.title_cn = policy_analysis_data.title_cn
            policy_error.status = 0
            
            # SAVE TO OUTPUT FILE
            # write_to_jsonl(policy_error,  filename_prefix='policy_error')
            # SAVE RECORD
            ok = self.db_manager.db_insert(
                table_name=constants.TableNames.TBL_POLICIES_ERRORS,
                data=policy_error
            )        
            return ok
        except Exception as e:
            logger.error(f"insert_policy_error: Error encountered. {e}")
            return False           
    
    ########################################################################################
    # RUN_ANALYSIS
    # returns: True if processing completed successfully, False if error encountered
    ########################################################################################
    def run_analysis(self, max_concurrency: int = 5):
        if not self.db_manager:
            logger.error("No db manager")
            return
        try:
            logger.info ("run analysis entered")
            
            # returns a list of stories to analyze 
            ok, story_list = self.preprocess()
            
            if not ok:
                logger.error("Error encountered")
                return False
            if not story_list or len(story_list) == 0:
                logger.info("No stories found")
                return True
            
            # Create a list of policy analysis data to evaluate
            policy_list: list[PolicyAnalysisData] = []
            for idx, story in enumerate(story_list, 1):
                new_policy: PolicyAnalysisData = PolicyAnalysisData()
                new_policy.id = idx
                new_policy.title_cn = story.title
                new_policy.chinese_original = story.story_text
                new_policy.time = story.date
                new_policy.dept_en = story.dept
                new_policy.source_url = story.link
                policy_list.append(new_policy)
                
            logger.info(f"{len(policy_list)} new policies")
            
            async def _run_all():
                sem = asyncio.Semaphore(max_concurrency)
            
                async def _run_one(policy: PolicyAnalysisData) -> bool:
                    async with sem:
                        try:
                            ok = await self.evaluate(policy)
                            if not ok:
                                ok = await self.evaluate(policy)
                            return ok
                        except Exception:
                            logger.exception(f"run_analysis: evaluate crashed id={policy.id}")
                            return False
                try:
                    tasks = [asyncio.create_task(_run_one(p)) for p in policy_list]
                    results = await asyncio.gather(*tasks)
                
                    ok_count = sum(1 for r in results if r is True)
                    fail_count = len(results) - ok_count
                    logger.info("run_analysis: complete ok=%s fail=%s total=%s", ok_count, fail_count, len(results))
                
                    # If you want run_analysis to fail if any policy failed:
                    return fail_count == 0
                finally:
                    # close async resources in the SAME event loop
                    if self.ai_master:
                        await self.aclose()

            # Run all evaluations with bounded concurrency
            overall_ok = asyncio.run(_run_all())
            
            return overall_ok
        except Exception as e:
            logger.error(f"Error encountered. {e}")
            return False
        finally:
            logger.info("*** ANALYSIS DONE")
            if self.db_manager:
                updt_record: AnalysisLastUpdt = AnalysisLastUpdt()
                updt_record.count = self.policy_count
                ok = self.db_manager.db_insert(table_name=constants.TableNames.TBL_ANALYSIS_LAST_UPDT, data=updt_record, exclude_list=['idx'])
                self.db_manager.db_close()
        
def test1():
    analysis = Analysis()
    analysis.run_analysis()
    
async def test2():
    try:
        db_manager = NeonManager(NeonConnectionMode.POOLER)
        if not db_manager:
            logger.info("No database connection")
            exit
        ok = db_manager.db_connect()
        if not ok:
            logger.info("No database connection")
            exit
            
        ref_data = load_ref_data_once(db_manager)
        if not ref_data:
            logger.info("No reference data found")
            return
        
        analysis = Analysis()
        
        table_name = constants.TableNames.TBL_POLICIES
        where_clause = {'id': 19492}
        ok, record = db_manager.db_select(table_name=table_name, dataclass=Policies, where=where_clause)
        if not ok or not record:
            logger.error("could not find record")
        record0 = record[0]
    
        # INITIALILZE REFERENCE DATA
        #ref_data_obj = RefData()
        #ref_data = load_ref_data_once()
        policy: Policies = Policies()
        policy.id = 1234
        policy.english_translation = record0.english_translation
        policy.source_url = record0.source_url
        policy.time = record0.time
        
        policy_analysis_data = PolicyAnalysisData()
        policy_analysis_data.id = policy.id
        policy_analysis_data.english_translation = policy.english_translation
        policy_analysis_data.source_url = policy.source_url
        policy_analysis_data.time = policy.time
        dept_id = record0.department_id
        policy_analysis_data.dept_en = next((code for code, info in ref_data.departments.items() if info.get('id') == dept_id), None)
        status = await analysis.full_ai_analysis(policy_analysis_data)
        db_manager.db_close()
    except Exception as e:
        print(f"Error encounter. {e}")
        pass
    finally:
        if "analysis" in locals() and analysis.ai_master:
            await analysis.aclose()
   
async def test3():
    db_manager = NeonManager(NeonConnectionMode.POOLER)
    if not db_manager:
        logger.info("No database connection")
    ok = db_manager.db_connect()
    if not ok:
        logger.info("No database connection")
        
    try:
        ref_data = load_ref_data_once(db_manager)
        if not ref_data:
            logger.info("No reference data found")
            return
        
        analysis = Analysis()
        
        policy_analysis_data = PolicyAnalysisData()
        table_name = constants.TableNames.TBL_POLICIES
        where_clause = {'id': 19492}
        ok, record = db_manager.db_select(table_name=table_name, dataclass=Policies, where=where_clause)
        if not ok or not record:
            logger.error("could not find record")
            return
        record0 = record[0]
        write_to_jsonl(record0,  filename_prefix='policy_old')
        
        policy_analysis_data.chinese_original = record0.chinese_original
        dept_id = record0.department_id
        policy_analysis_data.dept_en = next((code for code, info in ref_data.departments.items() if info.get('id') == dept_id), None)
        policy_analysis_data.source_url = record0.source_url
        policy_analysis_data.time = record0.time
        policy_analysis_data.id = 1
        ok = await analysis.evaluate(policy_analysis_data)
        db_manager.db_close()
        
    except Exception as e:
        print(f"Error encountered. {e}")
        pass
    
def test4():
    policy_analysis_data = read_from_jsonl('anlysdata')
    analysis: Analysis = Analysis()
    analysis.insert_policy(policy_analysis_data=policy_analysis_data)
    pass

# Test insert into error table
def test_error():
    filename_prefix='anlysdata'
    ok, raw = read_from_jsonl(filename_prefix=filename_prefix)
    if not ok:
        print("Error: Could not load data from {filename_prefix}")
        return    
    policy_analysis_data = PolicyAnalysisData(**raw)
    # print (policy_analysis_data.time)
    analysis: Analysis = Analysis()
    ok = analysis.insert_policy_error(policy_analysis_data=policy_analysis_data)
    print(f"Test insert into error table: {ok}")

def test_all():
    db_manager = NeonManager(NeonConnectionMode.POOLER)
    if not db_manager:
        logger.info("No database connection")
    ok = db_manager.db_connect()
    if not ok:
        logger.info("No database connection")          
    # Initialize database, clear errors and policies tables
    reload_all()
    ok, count = db_manager.db_count(constants.TableNames.TBL_STORIES_ALL_FINAL)
    if not ok:
        exit(0)
    logger.info(f"{constants.TableNames.TBL_STORIES_ALL_FINAL} contains {count} records")
    # initialize analysis
    analysis: Analysis = Analysis()
    analysis.run_analysis()
    ok, count = db_manager.db_count(constants.TableNames.TBL_STORIES_ALL_FINAL)
    if not ok:
        exit(0)
    logger.info(f"{constants.TableNames.TBL_STORIES_ALL_FINAL} contains {count} records")
    
if __name__ == "__main__":
    # test1()
    # asyncio.run(test2())
    # asyncio.run(test3())
    # test4()
    test_all()
    # test_error()