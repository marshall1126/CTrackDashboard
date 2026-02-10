import asyncio
from pathlib import Path
import sys
from typing import Optional

# Ensure project root is on sys.path (works regardless of current working directory)
PROJECT_ROOT = Path(__file__).resolve().parents[1]  # adjust if needed
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
   
from logger import get_logger
import tiktoken

logger = get_logger(__name__)
logger.propagate = False

from analysis_scripts.ai_master import AIMaster
from analysis_scripts.ai_model_params import AIModelParams

CHAT_GPT_4o_MINI = "gpt-4o-mini"
CHAT_GPT_5_MINI = "gpt-5-mini"
MODEL_TRANSLATE = CHAT_GPT_4o_MINI
TRANSLATION_CHUNK_SIZE = 6000
TOK_MAX_PROCESSABLE = 80000

enc = tiktoken.get_encoding("cl100k_base")

class Translator:
    def __init__(self, ai_master):
        self.ai_master = ai_master
        self.ai_model_params = AIModelParams()
    
    def count_tokens(self, text: str) -> int:
        """Counts tokens using the cl100k_base encoding."""
        if not text:
            return 0
        return len(enc.encode(text))
    
    def default_system_message(self) -> str:
        return """
      You are an expert Chinese-to-English translator specializing in official Chinese government and policy documents.
      Translate accurately, naturally, and consistently.
      Preserve proper names, dates, titles, and technical terms exactly.
      Output ONLY the English translation — no explanations, no Chinese, no markdown.
      """
    
    def user_message_first(self, text):
            user_message = f"""
            Translate the following text from an official Chinese government/policy document into natural, professional English.
    
            Preserve all proper names, titles, organizations, dates and technical terms exactly.
            Output only the English translation — no explanations, no notes, no markdown.
            
            Rules you **must** strictly follow:
            • Preserve **all** proper names, official titles, organizations, dates, laws, regulations and technical/specialized terms **exactly** as written
            • Use consistent, professional governmental English style
            • **Do NOT** add any introductions, explanations, summaries, footnotes, comments or notes of any kind
            • **Do NOT** include any Chinese text in the output
            • **Output only** the clean English translation — nothing else
    
            Chinese text:
    
            {text}
            """
            
            return user_message
        
        
    def user_message_following(self, text, translated_text):
        anchor = (translated_text or "")[-1200:]  # tweak 800–1500 as needed
        
        user_message = f"""
           You are continuing translation of the same official Chinese document.
           Maintain the same tone and terminology.
           
           Consistency anchor (tail of previous translation):
           {anchor}
           
           Next segment to translate:
           {text}
           
           Rules you **must** strictly follow:
           • Preserve **all** proper names, official titles, organizations, dates, laws, regulations and technical/specialized terms **exactly** as written
           • Use consistent, professional governmental English style
           • **Do NOT** add any introductions, explanations, summaries, footnotes, comments or notes of any kind
           • **Do NOT** include any Chinese text in the output
           • **Output only** the clean English translation — nothing else
           """
        return user_message
    
    def preprocess(self, text, story_id):
        token_count = self.count_tokens(text)
        logger.info(
                f"Story {story_id}: Text ~{len(text)} chars, {token_count} tokens."
            )
    
        if token_count > TOK_MAX_PROCESSABLE:
            logger.warning(
                    f"Story {story_id}: Text exceeds max processable ({token_count} > {TOK_MAX_PROCESSABLE}). Skipping."
                )
            return False
        
        return True
    

    # Your split_chinese_text method remains unchanged — it's excellent!
    def split_chinese_text(
      self,
      text: str,
      max_chunk_size: int = TRANSLATION_CHUNK_SIZE) -> list[str]:
        # ... (keep your current excellent version with 。 and \n support)
        # (No changes needed here)
        chunks = []
        start = 0
        while start < len(text):
            end = start + max_chunk_size
            if end >= len(text):
                remaining = text[start:].strip()
                if remaining:
                    chunks.append(remaining)
                break

            last_period = text.rfind('。', start, end)
            last_newline = text.rfind('\n', start, end)

            candidates = []
            if last_period != -1:
                candidates.append(last_period + 1)
            if last_newline != -1:
                candidates.append(last_newline + 1)

            chunk_end = max(candidates) if candidates else end
            chunk = text[start:chunk_end].strip()
            if chunk:
                chunks.append(chunk)
            start = chunk_end
        return chunks    
            
    async def translate(self,
                        text_cn: str,
                        story_id: Optional[str]
                        ) -> (bool, str, str):
        story_id = story_id or 'None'
        try:
            if not text_cn:
                errmsg = f"story_id: {story_id}: No text provided to translate"
                logger.info(errmsg)
                return True, None, errmsg
        
            ok = self.preprocess(text_cn, story_id)
            if not ok:
                errmsg = f"story_id: {story_id}: Preprocess failed"
                return False, None, errmsg
                
            chunks = self.split_chinese_text(text_cn)
            logger.info(f"Story {story_id}: Split into {len(chunks)} chunks")
        
            translated_chunks = []
            previous_translation = None
                        
            # SYSTEM MESSAGE
            system_message = self.default_system_message()
              
            # Process chunks sequentially to maintain context, but still async
            for i, chunk in enumerate(chunks, 1):
                if not previous_translation:
                    user_message = self.user_message_first(text=chunk)
                else:
                    user_message = self.user_message_following(text=chunk, translated_text=previous_translation)
                logger.info(f"Story {story_id}: Translating chunk {i}/{len(chunks)}...")
                # Build proper OpenAI-style messages list
                messages = [
                        {"role": "system", "content": system_message},
                        {"role": "user", "content": user_message},
                    ]                
                result = await self.ai_master.execute(messages=messages, model_params=self.ai_model_params)
                if result is None or not result.strip():
                    errmsg = f"Story {story_id}: Received empty or None response from model"
                    logger.warning(errmsg)
                    return False, None, errmsg
            
                translated = result.strip()
                      
                translated_chunks.append(translated)
                previous_translation = translated  # Carry forward context
        
            full_translation = "\n\n".join(translated_chunks)
            logger.info(f"Story {story_id}: Translation complete.")
            return True, full_translation, None

        except Exception as e:
            errmsg = f"story_id: {story_id}: Error during translation. {e}"
            logger.error(errmsg)
            return False, None, errmsg
        
if __name__ == "__main__":
    # text = "优质的营商环境对民营企业来说，犹如阳光雨露，是立业兴业的必要条件。2025年，我国优化营商环境的努力呈现出前所未有的清晰路径与亲和姿态，"
    text = "优质的营商环境对民营企业来说，犹如阳光雨露，是立业兴业的必要条件。2025年，我国优化营商环境的努力呈现出前所未有的清晰路径与亲和姿态，一条主线自上而下贯穿：国家层面以立法确立根本原则，部委层面以机制破解共性难题，地方层面以服务回应个性需求。法治保障、高位推动与一线服务相结合，共同构建起一个更加稳定、透明、可预期的市场生态。\n法治筑基民营经济促进法筑牢公平发展“压舱石”\n今年5月20日，《中华人民共和国民营经济促进法》（以下简称“民营经济促进法”）正式施行。作为我国首部针对民营经济发展的基础性法律，民营经济促进法系统回应了民营企业长期面临的公平竞争、融资困难、权益保护等核心问题，标志着我国民营经济法治化进程迈入新阶段，被视为给民营经济带来长期信心的“里程碑事件”。\n作为推动法律落地实施的核心牵头部门，国家发展改革委会同各方从宣传解读到实践落地、从权益保护到要素供给，构建起全方位的政策支撑体系，让民营经济在法治护航下焕发强劲活力。针对民营企业核心关切，在营造公平竞争市场环境、强化民营企业要素支撑、优化涉企服务等方面，已推出140余项配套制度，推动民营经济促进法的各项要求落实落细。比如，针对违法违规收费问题，国家发展改革委推动出台《关于建立健全涉企收费长效监管机制的指导意见》，健全涉企收费目录清单制度，清单之外一律不得收费，为杜绝违规收费明确边界。\n12月22日，全国人大常委会法工委向全国人大常委会报告2025年备案审查工作情况。报告显示，为全面贯彻实施民营经济促进法，全国人大常委会法工委推动修改、废止1466件各类规范性文件。\n徒法不足以自行。只有狠抓落实，立法的“含金量”才能变成稳定的预期、发展的信心。\n“就在法律颁布施行10天之后，最高人民法院就以法律第70条为裁判依据，在一起行政案件中判决政府部门向某民营企业赔偿800余万元。”这是11月27日国家发展改革委举行的月度新闻发布会上，国家发展改革委政策研究室副主任、委新闻发言人李超在介绍民营经济促进法施行情况时提及的一个典型案例。这也成为了民营经济促进法颁布实施半年多以来取得成效的一个缩影。\n一个案例胜过一打文件，通过把民营经济促进法的相关规定落实到具体案件中，不仅是解决一家民营企业、一位企业家的具体问题，更为广大民营企业和企业家吃下法治的“定心丸”。\n机制暖心政企“早餐会”打通诉求响应“直通车”\n12月26日，国家发展改革委主任郑栅洁主持召开民营企业座谈会，围绕贯彻落实中央经济工作会议决策部署，听取民营企业关于做好明年经济工作的意见建议。来自家电汽车零部件制造、农牧产品加工、教育信息化建设、生物医药研制等领域的部分民营企业负责人参加座谈会。\n就在12月1日，新修改的食品安全法正式施行，将婴幼儿配方液态乳纳入注册管理。中国发展改革报社记者了解到，这项开创性的制度安排，同样与国家发展改革委的一次座谈会有关。\n在国家发展改革委主任郑栅洁主持召开的一场民营企业座谈会上，飞鹤乳业相关负责人提到，希望明确婴幼儿配方液态乳产品监管要求。了解到相关诉求后，国家发展改革委综合一线调研、试验验证和征求意见等方面情况，推动将婴幼儿配方液态奶纳入注册管理范围。\n自2023年7月以来，类似的民营企业座谈会已经举行了20余场。由于这些座谈会大多是在早餐时间进行的，因此也被称为“早餐会”。交流中，听到的是企业的真实声音，解决的是企业的实际问题，构建的是亲清政商关系。\n天能控股集团有限公司董事长张天任也曾在一场早餐会上直言，电动自行车55公斤重量上限，限制了容量稍大但安全稳定的铅酸蓄电池的使用。国家发展改革委随即联动工信部、市场监管总局，历经8个月70余次调研、160余次技术研讨，最终推动新国标将电动自行车重量上限放宽至63公斤。\n这些被解决的问题并非个例。据国家发展改革委统计，截至第18次“早餐会”，民营企业共提出191条意见建议，解决了160多条。\n政企高效沟通背后，是一套成熟的治理机制在发力。2023年以来，国家发展改革委建立与民营企业常态化沟通交流机制，国家发展改革委主任郑栅洁先后主持召开了20余场座谈会，委党组成员和各司局与民营企业座谈交流500余次，各级发展改革部门召开民营企业座谈会2万余次。在这些沟通交流中，很多都是围绕民间投资主题而开展的，目的就是了解掌握情况、推动解决问题、主动服务企业。\n2024年12月，国家发展改革委门户网站正式上线开通民营经济发展综合服务平台。截至今年10月底，平台访问量已超过50万人次，通过平台办理的问题诉求5600余项。\n民营企业的难题往往跨领域、跨部门，单靠一个部门难以解决。为此，国家发展改革委推动成立了由43家部门单位组成的部际联席会议制度，“横向协同”解决难题；多次到地方召开现场会，推介投资项目、协调要素资源、听取政策建议等，“纵向联动”疏通堵点；建立了一套“问题收集—办理—反馈—跟踪问效”的闭环工作流程，坚持“有求必应”，及时反馈办理结果，即便一时解决不了的问题，也会把相关情况、工作打算向企业讲清楚。\n协同发力多维改革绘就营商环境“新图景”\n以民营经济促进法落地为牵引，各地同步深化数字赋能、监管优化、权益保障等多维改革，形成“法治+机制+服务”的营商环境优化合力，让民营经济活力充分迸发。\n在服务集约化方面，各地既做优传统服务，也敢闯新业态新领域。上海实现企业信息“一处变更、多处联动”、四川乐山上岗“政务服务体验员”、湖北十堰实现“跨域通办”等举措，持续打通数据壁垒，实现“数据多跑路、企业少跑腿”；深圳今年发布市场化、法治化、国际化三大营商环境优化工作方案，以精准政策组合拳激发市场活力。\n在市场准入突破方面，安徽合肥依托国际先进技术应用推进中心，打造“超级场景”为新技术提供试验场，破解新业态准入难题；海南则通过优化商业航天领域市场准入环境，打通商业航天全产业链发展通道。\n在监管协同化方面，甘肃嘉峪关“一业一查”、宁夏柔性执法、辽宁沈阳“一次进门查多件事”等模式，既破解多头监管难题，又传递执法温度。此外，多地出台优化营商环境条例，以法治规范推动监管提质，为经营主体营造稳定预期。\n在权益保障与要素支撑方面，安徽桐城“六尺巷”调解法、河南开封“放心消费险”等创新，构建起全链条保障体系；云南作为市场准入效能评估试点省份，通过精准排查整改旅行社审批等卡点问题，推动旅游市场经营主体恢复活力，让公平准入落到实处。\n优化营商环境没有终点。政府、企业和社会各界共同努力，持续破解难题、激发潜力，将为包括民营企业在内的各类经营主体培育发展沃土，为经济高质量发展提供坚实支撑。\n（中国发展改革报社记者 安宁）"
    
    
    ai_master = AIMaster()
    translator = Translator(ai_master)
    # Properly run the async translate method
    ok, trans_text, errmsg = asyncio.run(translator.translate(text, 
                story_id="TEST-2026-01-11"))   # ← very helpful when reading logs))
    if ok:
        print(f"Translated text: {trans_text}")
    else:
        print(f"Failed. errmsg={errmsg}")
    
    text = "优质的营商环境对民营企业来说，犹如阳光雨露，是立业兴业的必要条件。2025年，我国优化营商环境的努力呈现出前所未有的清晰路径与亲和姿态，一条主线自上而下贯穿：国家层面以立法确立根本原则，部委层面以机制破解共性难题，地方层面以服务回应个性需求。法治保障、高位推动与一线服务相结合，共同构建起一个更加稳定、透明、可预期的市场生态。\n法治筑基民营经济促进法筑牢公平发展“压舱石”\n今年5月20日，《中华人民共和国民营经济促进法》（以下简称“民营经济促进法”）正式施行。作为我国首部针对民营经济发展的基础性法律，民营经济促进法系统回应了民营企业长期面临的公平竞争、融资困难、权益保护等核心问题，标志着我国民营经济法治化进程迈入新阶段，被视为给民营经济带来长期信心的“里程碑事件”。\n作为推动法律落地实施的核心牵头部门，国家发展改革委会同各方从宣传解读到实践落地、从权益保护到要素供给，构建起全方位的政策支撑体系，让民营经济在法治护航下焕发强劲活力。针对民营企业核心关切，在营造公平竞争市场环境、强化民营企业要素支撑、优化涉企服务等方面，已推出140余项配套制度，推动民营经济促进法的各项要求落实落细。比如，针对违法违规收费问题，国家发展改革委推动出台《关于建立健全涉企收费长效监管机制的指导意见》，健全涉企收费目录清单制度，清单之外一律不得收费，为杜绝违规收费明确边界。\n12月22日，全国人大常委会法工委向全国人大常委会报告2025年备案审查工作情况。报告显示，为全面贯彻实施民营经济促进法，全国人大常委会法工委推动修改、废止1466件各类规范性文件。\n徒法不足以自行。只有狠抓落实，立法的“含金量”才能变成稳定的预期、发展的信心。\n“就在法律颁布施行10天之后，最高人民法院就以法律第70条为裁判依据，在一起行政案件中判决政府部门向某民营企业赔偿800余万元。”这是11月27日国家发展改革委举行的月度新闻发布会上，国家发展改革委政策研究室副主任、委新闻发言人李超在介绍民营经济促进法施行情况时提及的一个典型案例。这也成为了民营经济促进法颁布实施半年多以来取得成效的一个缩影。\n一个案例胜过一打文件，通过把民营经济促进法的相关规定落实到具体案件中，不仅是解决一家民营企业、一位企业家的具体问题，更为广大民营企业和企业家吃下法治的“定心丸”。\n机制暖心政企“早餐会”打通诉求响应“直通车”\n12月26日，国家发展改革委主任郑栅洁主持召开民营企业座谈会，围绕贯彻落实中央经济工作会议决策部署，听取民营企业关于做好明年经济工作的意见建议。来自家电汽车零部件制造、农牧产品加工、教育信息化建设、生物医药研制等领域的部分民营企业负责人参加座谈会。\n就在12月1日，新修改的食品安全法正式施行，将婴幼儿配方液态乳纳入注册管理。中国发展改革报社记者了解到，这项开创性的制度安排，同样与国家发展改革委的一次座谈会有关。\n在国家发展改革委主任郑栅洁主持召开的一场民营企业座谈会上，飞鹤乳业相关负责人提到，希望明确婴幼儿配方液态乳产品监管要求。了解到相关诉求后，国家发展改革委综合一线调研、试验验证和征求意见等方面情况，推动将婴幼儿配方液态奶纳入注册管理范围。\n自2023年7月以来，类似的民营企业座谈会已经举行了20余场。由于这些座谈会大多是在早餐时间进行的，因此也被称为“早餐会”。交流中，听到的是企业的真实声音，解决的是企业的实际问题，构建的是亲清政商关系。\n天能控股集团有限公司董事长张天任也曾在一场早餐会上直言，电动自行车55公斤重量上限，限制了容量稍大但安全稳定的铅酸蓄电池的使用。国家发展改革委随即联动工信部、市场监管总局，历经8个月70余次调研、160余次技术研讨，最终推动新国标将电动自行车重量上限放宽至63公斤。\n这些被解决的问题并非个例。据国家发展改革委统计，截至第18次“早餐会”，民营企业共提出191条意见建议，解决了160多条。\n政企高效沟通背后，是一套成熟的治理机制在发力。2023年以来，国家发展改革委建立与民营企业常态化沟通交流机制，国家发展改革委主任郑栅洁先后主持召开了20余场座谈会，委党组成员和各司局与民营企业座谈交流500余次，各级发展改革部门召开民营企业座谈会2万余次。在这些沟通交流中，很多都是围绕民间投资主题而开展的，目的就是了解掌握情况、推动解决问题、主动服务企业。\n2024年12月，国家发展改革委门户网站正式上线开通民营经济发展综合服务平台。截至今年10月底，平台访问量已超过50万人次，通过平台办理的问题诉求5600余项。\n民营企业的难题往往跨领域、跨部门，单靠一个部门难以解决。为此，国家发展改革委推动成立了由43家部门单位组成的部际联席会议制度，“横向协同”解决难题；多次到地方召开现场会，推介投资项目、协调要素资源、听取政策建议等，“纵向联动”疏通堵点；建立了一套“问题收集—办理—反馈—跟踪问效”的闭环工作流程，坚持“有求必应”，及时反馈办理结果，即便一时解决不了的问题，也会把相关情况、工作打算向企业讲清楚。\n协同发力多维改革绘就营商环境“新图景”\n以民营经济促进法落地为牵引，各地同步深化数字赋能、监管优化、权益保障等多维改革，形成“法治+机制+服务”的营商环境优化合力，让民营经济活力充分迸发。\n在服务集约化方面，各地既做优传统服务，也敢闯新业态新领域。上海实现企业信息“一处变更、多处联动”、四川乐山上岗“政务服务体验员”、湖北十堰实现“跨域通办”等举措，持续打通数据壁垒，实现“数据多跑路、企业少跑腿”；深圳今年发布市场化、法治化、国际化三大营商环境优化工作方案，以精准政策组合拳激发市场活力。\n在市场准入突破方面，安徽合肥依托国际先进技术应用推进中心，打造“超级场景”为新技术提供试验场，破解新业态准入难题；海南则通过优化商业航天领域市场准入环境，打通商业航天全产业链发展通道。\n在监管协同化方面，甘肃嘉峪关“一业一查”、宁夏柔性执法、辽宁沈阳“一次进门查多件事”等模式，既破解多头监管难题，又传递执法温度。此外，多地出台优化营商环境条例，以法治规范推动监管提质，为经营主体营造稳定预期。\n在权益保障与要素支撑方面，安徽桐城“六尺巷”调解法、河南开封“放心消费险”等创新，构建起全链条保障体系；云南作为市场准入效能评估试点省份，通过精准排查整改旅行社审批等卡点问题，推动旅游市场经营主体恢复活力，让公平准入落到实处。\n优化营商环境没有终点。政府、企业和社会各界共同努力，持续破解难题、激发潜力，将为包括民营企业在内的各类经营主体培育发展沃土，为经济高质量发展提供坚实支撑。\n（中国发展改革报社记者 安宁）"