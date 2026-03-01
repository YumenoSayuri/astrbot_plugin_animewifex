from astrbot.api.all import *
from astrbot.api.star import StarTools
from astrbot.api.event import filter
from datetime import datetime, timedelta
from typing import Tuple
from urllib.parse import urljoin
import random
import os
import re
import json
import aiohttp
import unicodedata

PLUGIN_DIR = StarTools.get_data_dir("astrbot_plugin_animewifex")
CONFIG_DIR = os.path.join(PLUGIN_DIR, "config")
IMG_DIR = os.path.join(PLUGIN_DIR, "img", "wife")
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(IMG_DIR, exist_ok=True)
NTR_STATUS_FILE = os.path.join(CONFIG_DIR, "ntr_status.json")
NTR_RECORDS_FILE = os.path.join(CONFIG_DIR, "ntr_records.json")
CHANGE_RECORDS_FILE = os.path.join(CONFIG_DIR, "change_records.json")
RESET_SHARED_FILE = os.path.join(CONFIG_DIR, "reset_shared_records.json")
SWAP_REQUESTS_FILE = os.path.join(CONFIG_DIR, "swap_requests.json")
SWAP_LIMIT_FILE = os.path.join(CONFIG_DIR, "swap_limit_records.json")
NTR_CD_FILE = os.path.join(CONFIG_DIR, "ntr_cd.json")


def get_today():
    # 获取当前上海时区日期字符串
    utc_now = datetime.utcnow()
    return (utc_now + timedelta(hours=8)).date().isoformat()


def load_json(path):
    # 安全加载 JSON 文件
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}


def save_json(path, data):
    # 保存数据到 JSON 文件
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


ntr_statuses = {}
ntr_records = {}
change_records = {}
swap_requests = {}
swap_limit_records = {}
ntr_cd = {}
load_ntr_statuses = lambda: globals().update(ntr_statuses=load_json(NTR_STATUS_FILE))
load_ntr_records = lambda: globals().update(ntr_records=load_json(NTR_RECORDS_FILE))
load_ntr_cd = lambda: globals().update(ntr_cd=load_json(NTR_CD_FILE))
save_ntr_cd = lambda: save_json(NTR_CD_FILE, ntr_cd)


def load_change_records():
    raw = load_json(CHANGE_RECORDS_FILE)
    change_records.clear()
    for gid, users in raw.items():
        change_records[gid] = {}
        for uid, rec in users.items():
            if isinstance(rec, str):
                change_records[gid][uid] = {"date": rec, "count": 1}
            else:
                change_records[gid][uid] = rec


save_ntr_statuses = lambda: save_json(NTR_STATUS_FILE, ntr_statuses)
save_ntr_records = lambda: save_json(NTR_RECORDS_FILE, ntr_records)
save_change_records = lambda: save_json(CHANGE_RECORDS_FILE, change_records)


def load_group_config(group_id: str) -> dict:
    path = os.path.join(CONFIG_DIR, f"{group_id}.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # 兼容旧数据格式，转换为新格式
            for uid, value in data.items():
                if isinstance(value, list) and len(value) == 3:
                    # 旧格式：[wife_name, date, nickname]
                    data[uid] = {"drawn": value, "ntr": []}
                elif isinstance(value, dict):
                    # 兼容旧的新格式（ntr可能是None或单个值）
                    if "ntr" in value and value["ntr"] is not None and not isinstance(value["ntr"], list):
                        # 单个ntr值，转换为列表
                        value["ntr"] = [value["ntr"]]
                    elif "ntr" not in value or value["ntr"] is None:
                        value["ntr"] = []
            return data
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_group_config(
    group_id: str, user_id: str, wife_name: str, date: str, nickname: str, config: dict
):
    if user_id not in config:
        config[user_id] = {"drawn": None, "ntr": []}
    if config[user_id].get("drawn") is None:
        config[user_id]["drawn"] = [wife_name, date, nickname]
    if config[user_id].get("ntr") is None:
        config[user_id]["ntr"] = []
    path = os.path.join(CONFIG_DIR, f"{group_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def save_group_config(group_id: str, config: dict):
    path = os.path.join(CONFIG_DIR, f"{group_id}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)

def clean_expired_ntr_records(gid: str, config: dict, today: str):
    """清理群配置中所有用户过期的牛来的老婆记录"""
    changed = False
    for uid, user_data in config.items():
        if isinstance(user_data, dict):
            ntr_list = user_data.get("ntr", [])
            if ntr_list:
                # 过滤掉日期不等于今天的记录
                new_ntr_list = [
                    ntr_wife for ntr_wife in ntr_list
                    if isinstance(ntr_wife, list) and len(ntr_wife) > 1 and ntr_wife[1] == today
                ]
                if len(new_ntr_list) != len(ntr_list):
                    config[uid]["ntr"] = new_ntr_list
                    changed = True
    if changed:
        save_group_config(gid, config)

def check_ntr_cd(gid: str, uid: str, cd_duration: int = 3600, target_nick: str = None) -> Tuple[bool, str, int]:
    """检查用户是否在CD中，返回(是否在CD中, 提示信息, 剩余CD时间秒数)
    
    Args:
        gid: 群ID
        uid: 用户ID
        cd_duration: CD时长（秒），默认3600秒
        target_nick: 目标用户昵称，用于替换提示信息中的"你"
    """
    grp = ntr_cd.get(gid, {})
    if uid not in grp:
        return False, "", 0
    cd_info = grp[uid]
    cd_time = datetime.fromisoformat(cd_info["time"])
    now = datetime.utcnow() + timedelta(hours=8)
    elapsed = (now - cd_time).total_seconds()
    remaining = max(0, cd_duration - elapsed)
    
    if elapsed < cd_duration:
        ntr_user_id = cd_info.get("ntr_user_id", "未知用户")
        # 尝试获取牛走的人的名字
        cfg = load_group_config(gid)
        ntr_nick = "未知用户"
        if ntr_user_id in cfg:
            user_data = cfg[ntr_user_id]
            # 支持新数据结构
            if isinstance(user_data, dict):
                drawn = user_data.get("drawn")
                ntr_list = user_data.get("ntr", [])
                if drawn:
                    ntr_nick = drawn[2]
                elif ntr_list and isinstance(ntr_list, list) and len(ntr_list) > 0:
                    ntr_nick = ntr_list[0][2] if isinstance(ntr_list[0], list) and len(ntr_list[0]) > 2 else "未知用户"
            # 兼容旧格式
            elif isinstance(user_data, list) and len(user_data) > 2:
                ntr_nick = user_data[2]
        
        # 使用target_nick替换"你"，如果没有提供则使用"你"
        target_name = target_nick if target_nick else "你"
        # 格式化剩余时间
        hours = int(remaining // 3600)
        minutes = int((remaining % 3600) // 60)
        seconds = int(remaining % 60)
        if hours > 0:
            time_str = f"{hours}小时{minutes}分{seconds}秒"
        elif minutes > 0:
            time_str = f"{minutes}分{seconds}秒"
        else:
            time_str = f"{seconds}秒"
        return True, f"{target_name}的老婆已被\"{ntr_nick}\"牛走，剩余CD时间：{time_str}~", int(remaining)
    else:
        # CD已过期，清除记录
        del grp[uid]
        if not grp:
            del ntr_cd[gid]
        else:
            ntr_cd[gid] = grp
        save_ntr_cd()
        return False, "", 0


def load_swap_requests():
    raw = load_json(SWAP_REQUESTS_FILE)
    today = get_today()
    cleaned = {}
    for gid, reqs in raw.items():
        valid = {}
        for uid, rec in reqs.items():
            if rec.get("date") == today:
                valid[uid] = rec
        if valid:
            cleaned[gid] = valid
    globals()["swap_requests"] = cleaned
    if raw != cleaned:
        save_json(SWAP_REQUESTS_FILE, cleaned)


save_swap_requests = lambda: save_json(SWAP_REQUESTS_FILE, swap_requests)


def load_swap_limit_records():
    globals()["swap_limit_records"] = load_json(SWAP_LIMIT_FILE)


save_swap_limit_records = lambda: save_json(SWAP_LIMIT_FILE, swap_limit_records)
load_ntr_statuses()
load_ntr_records()
load_change_records()
load_swap_requests()
load_swap_limit_records()
load_ntr_cd()


@register(
    "astrbot_plugin_animewifex",
    "辉宝",
    "群二次元老婆插件修改版",
    "1.7.0",
    "https://github.com/YumenoSayuri/astrbot_plugin_animewifex",
)
class WifePlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        # 配置参数初始化
        self.ntr_max = config.get("ntr_max")
        self.ntr_possibility = config.get("ntr_possibility")
        self.change_max_per_day = config.get("change_max_per_day")
        self.reset_max_uses_per_day = config.get("reset_max_uses_per_day")
        self.reset_success_rate = config.get("reset_success_rate")
        self.reset_mute_duration = config.get("reset_mute_duration")
        self.image_base_url = config.get("image_base_url")
        self.swap_max_per_day = config.get("swap_max_per_day")
        self.ntr_both_probability = config.get("ntr_both_probability", 0.30)
        self.ntr_cd_duration = config.get("ntr_cd_duration", 3600)
        
        # ========== 纯爱模式配置 ==========
        self.pure_love_enabled = str(config.get("pure_love_enabled", True)).lower() in ("true", "1", "yes")
        self.pure_love_source = config.get("pure_love_source", "尘白禁区").strip()
        try:
            self.pure_love_runaway_prob = float(config.get("pure_love_runaway_prob", 0.30))
        except (TypeError, ValueError):
            self.pure_love_runaway_prob = 0.30
        try:
            self.pure_love_reward_days = max(1, int(config.get("pure_love_reward_days", 3)))
        except (TypeError, ValueError):
            self.pure_love_reward_days = 3
        # 纯爱奖励排除关键词列表
        blacklist_raw = config.get("pure_love_blacklist", "")
        if isinstance(blacklist_raw, str) and blacklist_raw.strip():
            self.pure_love_blacklist = [kw.strip() for kw in blacklist_raw.split(",") if kw.strip()]
        else:
            self.pure_love_blacklist = []
        
        # 命令与处理函数映射（用于无前缀触发）
        self.commands = {
            "抽老婆": self.animewife,
            "牛老婆": self.ntr_wife,
            "查老婆": self.search_wife,
            "切换ntr开关状态": self.switch_ntr,
            "发老婆": self.give_wife,
            "拆散": self.breakup_wife,
            "解除保护": self.unprotect_wife,
            "换老婆": self.change_wife,
            "重置牛": self.reset_ntr,
            "重置换": self.reset_change_wife,
            "交换老婆": self.swap_wife,
            "同意交换": self.agree_swap_wife,
            "拒绝交换": self.reject_swap_wife,
            "查看交换请求": self.view_swap_requests,
        }
        self.admins = self.load_admins()

    def load_admins(self):
        # 加载管理员列表
        path = os.path.join("data", "cmd_config.json")
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                cfg = json.load(f)
                return cfg.get("admins_id", [])
        except:
            return []

    def parse_at_target(self, event):
        # 解析@目标用户
        for comp in event.message_obj.message:
            if isinstance(comp, At):
                return str(comp.qq)
        return None

    def parse_target(self, event):
        # 解析命令目标用户
        target = self.parse_at_target(event)
        if target:
            return target
        msg = event.message_str.strip()
        if msg.startswith("牛老婆") or msg.startswith("查老婆"):
            name = msg.split(maxsplit=1)[-1]
            if name:
                group_id = str(event.message_obj.group_id)
                cfg = load_group_config(group_id)
                for uid, user_data in cfg.items():
                    # 支持新数据结构
                    if isinstance(user_data, dict):
                        drawn = user_data.get("drawn")
                        ntr_list = user_data.get("ntr", [])
                        if drawn:
                            nick = drawn[2]
                        elif ntr_list and isinstance(ntr_list, list) and len(ntr_list) > 0:
                            ntr_wife = ntr_list[0]
                            nick = ntr_wife[2] if isinstance(ntr_wife, list) and len(ntr_wife) > 2 else None
                        else:
                            nick = None
                    else:
                        # 兼容旧格式
                        nick = user_data[2] if isinstance(user_data, list) and len(user_data) > 2 else None
                    if nick and re.search(re.escape(name), nick, re.IGNORECASE):
                        return uid
        return None

    def build_image_component(self, img_name: str):
        if not img_name:
            return None
        if img_name.startswith("http://") or img_name.startswith("https://"):
            return Image.fromURL(img_name)
        if img_name.startswith("file:///"):
            file_path = img_name[8:]
            if os.path.exists(file_path):
                return Image.fromFileSystem(file_path)
            return Image.fromURL(img_name)
        if os.path.isabs(img_name) and os.path.exists(img_name):
            return Image.fromFileSystem(img_name)
        path = os.path.join(IMG_DIR, img_name)
        if os.path.exists(path):
            return Image.fromFileSystem(path)
        if self.image_base_url:
            url = urljoin(self.image_base_url, img_name)
            if url.startswith("http://") or url.startswith("https://"):
                return Image.fromURL(url)
        return None

    def _check_group(self, event: AstrMessageEvent) -> bool:
        """检查是否为群聊消息"""
        return hasattr(event.message_obj, "group_id")

    def _get_raw_text(self, event: AstrMessageEvent) -> str:
        """获取原始消息文本（包含命令前缀）"""
        # 尝试从消息组件中获取原始文本
        if hasattr(event.message_obj, "message") and event.message_obj.message:
            for comp in event.message_obj.message:
                if isinstance(comp, Plain):
                    return comp.text.strip()
        # 回退到 message_str
        return event.message_str.strip()

    # ==================== 纯爱模式辅助函数 ====================

    def _is_pure_love_source(self, img_name: str) -> bool:
        """判断角色卡是否属于纯爱触发出处（如'尘白禁区'）
        
        文件名格式: 出处!角色名.扩展名
        检查出处部分是否包含 pure_love_source 关键词
        """
        if not self.pure_love_enabled or not self.pure_love_source:
            return False
        name = os.path.splitext(img_name)[0]
        if "!" in name:
            source, _ = name.split("!", 1)
            return self.pure_love_source in source
        return False

    def _ensure_user_data(self, cfg: dict, uid: str) -> dict:
        """确保用户数据结构完整，返回用户数据引用"""
        if uid not in cfg:
            cfg[uid] = {"drawn": None, "ntr": []}
        user_data = cfg[uid]
        if not isinstance(user_data, dict):
            cfg[uid] = {"drawn": None, "ntr": []}
            user_data = cfg[uid]
        if "ntr" not in user_data or user_data["ntr"] is None:
            user_data["ntr"] = []
        return user_data

    def _get_pure_love_info(self, user_data: dict) -> dict:
        """获取用户纯爱模式信息
        
        返回 {
            "active": bool,              # 纯爱模式是否生效
            "start_date": str,           # 纯爱开始日期
            "days": int,                 # 已坚持天数
            "runaway_date": str,         # 跑路日期（当天禁抽）
            "bonus_wives": list,         # 奖励老婆列表（永久保留）
            "bonus_available": int,      # 当前可用的奖励抽老婆次数（0或1）
            "last_reward_day": int,      # 上次获得奖励时的纯爱天数
        }
        """
        return {
            "active": user_data.get("pure_love", False),
            "start_date": user_data.get("pure_love_start", ""),
            "days": user_data.get("pure_love_days", 0),
            "runaway_date": user_data.get("pure_love_runaway", ""),
            "bonus_wives": user_data.get("pure_love_bonus_wives", []),
            "bonus_available": user_data.get("pure_love_bonus_available", 0),
            "last_reward_day": user_data.get("pure_love_last_reward_day", 0),
        }

    def _set_pure_love(self, user_data: dict, start_date: str):
        """激活纯爱模式"""
        user_data["pure_love"] = True
        user_data["pure_love_start"] = start_date
        user_data["pure_love_days"] = 1  # 第一天
        user_data["pure_love_runaway"] = ""
        if "pure_love_bonus_wives" not in user_data:
            user_data["pure_love_bonus_wives"] = []
        user_data["pure_love_bonus_available"] = 0  # 初始无可用奖励
        user_data["pure_love_last_reward_day"] = 0  # 初始无上次奖励天数

    def _clear_pure_love(self, user_data: dict):
        """清除纯爱模式（不删除奖励老婆，由跑路逻辑单独处理）"""
        user_data["pure_love"] = False
        user_data["pure_love_start"] = ""
        user_data["pure_love_days"] = 0
        user_data["pure_love_bonus_available"] = 0
        user_data["pure_love_last_reward_day"] = 0

    def _draw_bonus_wife(self, existing_wives: set) -> str:
        """为纯爱用户抽取一个奖励老婆（排除黑名单和已拥有的）
        
        Args:
            existing_wives: 已拥有的老婆文件名集合
            
        Returns:
            抽到的老婆文件名，如果没有可选则返回空字符串
        """
        local_imgs = os.listdir(IMG_DIR)
        if not local_imgs:
            return ""
        
        # 过滤：排除已拥有 + 排除黑名单
        candidates = []
        for img in local_imgs:
            if img in existing_wives:
                continue
            name = os.path.splitext(img)[0]
            # 检查黑名单关键词
            blocked = False
            for kw in self.pure_love_blacklist:
                if kw in name:
                    blocked = True
                    break
            if not blocked:
                candidates.append(img)
        
        if not candidates:
            # 如果过滤后没有候选，放宽到只排除已拥有
            candidates = [img for img in local_imgs if img not in existing_wives]
        
        if not candidates:
            return ""
        
        return random.choice(candidates)

    def _handle_pure_love_runaway(self, user_data: dict, today: str):
        """处理纯爱老婆跑路
        
        - 清除 drawn（尘白老婆跑了）
        - 清除纯爱状态（含奖励计数器）
        - 设置跑路日期（当天禁抽）
        - 清除所有奖励老婆（跑路 = 所有老婆一起跑）
        - 保留 ntr 来的老婆
        """
        user_data["drawn"] = None
        self._clear_pure_love(user_data)  # 会清除 bonus_available 和 last_reward_day
        user_data["pure_love_runaway"] = today
        user_data["pure_love_bonus_wives"] = []
        # protected 也清除
        user_data["protected"] = False

    def _format_wife_display(self, img_name: str) -> str:
        """格式化老婆显示名（出处+角色名）"""
        name = os.path.splitext(img_name)[0]
        if "!" in name:
            source, chara = name.split("!", 1)
            return f"来自《{source}》的{chara}"
        return name

    # ==================== 消息分发 ====================

    @event_message_type(EventMessageType.ALL)
    async def on_all_messages(self, event: AstrMessageEvent):
        """消息分发，根据命令调用对应方法（仅处理无前缀触发）"""
        if not hasattr(event.message_obj, "group_id"):
            return
        
        # 获取原始消息文本，检查是否以 / 开头
        raw_text = self._get_raw_text(event)
        # 如果原始消息以 / 开头，跳过（由 @filter.command 装饰器处理，避免重复触发）
        if raw_text.startswith("/"):
            return
        
        text = event.message_str.strip()
        for cmd, func in self.commands.items():
            if text.startswith(cmd):
                async for res in func(event):
                    yield res
                return  # 匹配到命令后直接返回，不再继续

    # ==================== 核心命令 ====================

    @filter.command("抽老婆")
    async def animewife(self, event: AstrMessageEvent):
        """每天随机抽取一张二次元老婆图片，每日限抽一次
        
        纯爱模式逻辑：
        - 如果已有纯爱老婆且未跑路：保持不变，天数+1，检查奖励
        - 如果是新抽到尘白角色：激活纯爱模式
        - 如果当天跑路了：禁止抽老婆
        """
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        # 抽老婆主逻辑
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        
        # 检查CD状态
        in_cd, cd_msg, _ = check_ntr_cd(gid, uid, self.ntr_cd_duration, nick)
        if in_cd:
            yield event.plain_result(cd_msg)
            return
        
        cfg = load_group_config(gid)
        # 清理过期的牛来的老婆记录
        clean_expired_ntr_records(gid, cfg, today)
        user_data = self._ensure_user_data(cfg, uid)
        drawn_wife = user_data.get("drawn")
        ntr_wife_list = user_data.get("ntr", [])
        if not isinstance(ntr_wife_list, list):
            ntr_wife_list = [ntr_wife_list] if ntr_wife_list else []
        
        pl_info = self._get_pure_love_info(user_data)
        
        # ===== 纯爱模式：跑路当天禁抽 =====
        if pl_info["runaway_date"] == today:
            yield event.plain_result(
                f"{nick}，你的纯爱老婆今天跑路了……今天无法再抽老婆了，只能去牛别人的哦~"
            )
            return
        
        # ===== 纯爱模式：老婆还在，跨天保持 =====
        if pl_info["active"] and drawn_wife is not None:
            # 纯爱老婆还在，检查是否需要刷新天数
            if drawn_wife[1] != today:
                # 新的一天到了，天数+1，更新日期但保持老婆不变
                new_days = pl_info["days"] + 1
                user_data["pure_love_days"] = new_days
                drawn_wife[1] = today  # 更新日期标记
                drawn_wife[2] = nick   # 更新昵称（可能改名了）
                user_data["drawn"] = drawn_wife
                
                # ===== 纯爱奖励机制（修正版）=====
                # 检查是否应该获得新的奖励次数
                # 逻辑：从上次领奖后算起，每坚持 N 天获得 1 次奖励机会
                last_reward_day = user_data.get("pure_love_last_reward_day", 0)
                bonus_available = user_data.get("pure_love_bonus_available", 0)
                days_since_last_reward = new_days - last_reward_day
                
                if bonus_available == 0 and days_since_last_reward >= self.pure_love_reward_days:
                    # 距上次领奖已满 N 天，获得 1 次奖励机会
                    user_data["pure_love_bonus_available"] = 1
                    bonus_available = 1
                
                # 如果有可用奖励次数，立即使用：额外抽一个老婆
                current_bonus = user_data.get("pure_love_bonus_wives", [])
                if not isinstance(current_bonus, list):
                    current_bonus = []
                
                bonus_msgs = []
                if bonus_available > 0:
                    # 收集已有的老婆名，避免重复
                    existing = set()
                    if drawn_wife:
                        existing.add(drawn_wife[0])
                    for nw in ntr_wife_list:
                        if isinstance(nw, list) and len(nw) > 0:
                            existing.add(nw[0])
                    for bw in current_bonus:
                        if isinstance(bw, list) and len(bw) > 0:
                            existing.add(bw[0])
                    
                    bonus_img = self._draw_bonus_wife(existing)
                    if bonus_img:
                        current_bonus.append([bonus_img, today, nick])
                        display = self._format_wife_display(bonus_img)
                        bonus_msgs.append(display)
                    
                    # 使用后：奖励次数归 0，记录本次领奖时的天数
                    user_data["pure_love_bonus_available"] = 0
                    user_data["pure_love_last_reward_day"] = new_days
                
                user_data["pure_love_bonus_wives"] = current_bonus
                # 日期变化时清除保护状态（管理员派发的保护不跨天）—— 但纯爱保护不清除
                if user_data.get("protected", False) and not pl_info["active"]:
                    user_data["protected"] = False
                
                save_group_config(gid, cfg)
                
                # 构建消息
                img = drawn_wife[0]
                name = os.path.splitext(img)[0]
                if "!" in name:
                    source, chara = name.split("!", 1)
                    text = f"💕 {nick}，你的纯爱老婆{chara}还在身边哦~已坚持 {new_days} 天！"
                else:
                    text = f"💕 {nick}，你的纯爱老婆{name}还在身边哦~已坚持 {new_days} 天！"
                
                # 距离下次奖励
                current_last_reward = user_data.get("pure_love_last_reward_day", 0)
                days_to_next = self.pure_love_reward_days - (new_days - current_last_reward)
                if days_to_next > 0:
                    text += f"\n📅 距离下次纯爱奖励还有 {days_to_next} 天"
                elif days_to_next == 0 and user_data.get("pure_love_bonus_available", 0) > 0:
                    text += f"\n🎁 你有 1 次纯爱奖励可用，明天抽老婆时自动领取！"
                
                # 显示奖励消息
                if bonus_msgs:
                    text += f"\n🎁 纯爱奖励！你获得了新老婆：{'、'.join(bonus_msgs)}"
                
                chain = [Plain(text)]
                main_image = self.build_image_component(img)
                if main_image:
                    chain.append(main_image)
                
                # 显示新抽到的奖励老婆图片
                if bonus_msgs:
                    for bw in current_bonus[-len(bonus_msgs):]:
                        if isinstance(bw, list) and len(bw) > 0:
                            bw_img = self.build_image_component(bw[0])
                            if bw_img:
                                chain.append(bw_img)
                
                # 显示牛来的老婆
                if ntr_wife_list:
                    ntr_texts = []
                    for ntr_wife in ntr_wife_list:
                        if not isinstance(ntr_wife, list) or len(ntr_wife) < 1:
                            continue
                        ntr_texts.append(self._format_wife_display(ntr_wife[0]))
                        ntr_image = self.build_image_component(ntr_wife[0])
                        if ntr_image:
                            chain.append(ntr_image)
                    if ntr_texts:
                        text += f"\n你还有{len(ntr_texts)}个牛来的老婆：{', '.join(ntr_texts)}~"
                        chain[0] = Plain(text)
                
                # 显示已有的奖励老婆信息
                existing_bonus_display = []
                for bw in current_bonus:
                    if isinstance(bw, list) and len(bw) > 0:
                        existing_bonus_display.append(self._format_wife_display(bw[0]))
                if existing_bonus_display and not bonus_msgs:
                    text += f"\n🎁 纯爱奖励老婆：{'、'.join(existing_bonus_display)}"
                    chain[0] = Plain(text)
                
                try:
                    yield event.chain_result(chain)
                except:
                    yield event.plain_result(text)
                return
            else:
                # 今天已经看过了（日期一样），直接展示
                pass  # 继续到下面的通用展示逻辑
        
        # ===== 普通逻辑：如果今天还没抽，重新抽取 =====
        if drawn_wife is None or drawn_wife[1] != today:
            # 日期变化时清除保护状态（管理员派发的保护不跨天）
            if user_data.get("protected", False) and not pl_info["active"]:
                user_data["protected"] = False
            
            # 如果之前有纯爱状态但老婆没了（不应该发生，但防御性处理）
            if pl_info["active"] and drawn_wife is None:
                self._clear_pure_love(user_data)
                pl_info = self._get_pure_love_info(user_data)
            
            local_imgs = os.listdir(IMG_DIR)
            if local_imgs:
                img = random.choice(local_imgs)
            else:
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.get(self.image_base_url) as resp:
                            text = await resp.text()
                            img = random.choice(text.splitlines())
                except:
                    yield event.plain_result("抱歉，今天的老婆获取失败了，请稍后再试~")
                    return
            
            user_data["drawn"] = [img, today, nick]
            
            # 检查是否触发纯爱模式
            if self._is_pure_love_source(img):
                self._set_pure_love(user_data, today)
                user_data["protected"] = True  # 纯爱自动保护
            
            # 清除旧的奖励老婆（如果不再是纯爱模式）
            if not user_data.get("pure_love", False):
                user_data["pure_love_bonus_wives"] = []
            
            save_group_config(gid, cfg)
            drawn_wife = user_data["drawn"]
        
        # ===== 通用展示逻辑 =====
        img = drawn_wife[0]
        name = os.path.splitext(img)[0]
        is_pure_love = user_data.get("pure_love", False)
        pl_days = user_data.get("pure_love_days", 0)
        
        if "!" in name:
            source, chara = name.split("!", 1)
            if is_pure_love:
                text = f"💕 {nick}，你今天的老婆是来自《{source}》的{chara}，纯爱模式已激活！请好好珍惜哦~"
                if pl_days > 0:
                    text += f"\n💕 纯爱坚持 {pl_days} 天"
                    last_rwd = user_data.get("pure_love_last_reward_day", 0)
                    days_to_next = self.pure_love_reward_days - (pl_days - last_rwd)
                    if user_data.get("pure_love_bonus_available", 0) > 0:
                        text += f" | 🎁 有1次奖励待领取"
                    elif days_to_next > 0:
                        text += f" | 距下次奖励 {days_to_next} 天"
            else:
                text = f"{nick}，你今天的老婆是来自《{source}》的{chara}，请好好珍惜哦~"
        else:
            if is_pure_love:
                text = f"💕 {nick}，你今天的老婆是{name}，纯爱模式已激活！请好好珍惜哦~"
            else:
                text = f"{nick}，你今天的老婆是{name}，请好好珍惜哦~"
        
        # 构建图片链
        chain = [Plain(text)]
        
        # 添加抽到的老婆图片
        main_image = self.build_image_component(img)
        if main_image:
            chain.append(main_image)
        
        # 显示奖励老婆
        bonus_wives = user_data.get("pure_love_bonus_wives", [])
        if bonus_wives and isinstance(bonus_wives, list):
            bonus_texts = []
            for bw in bonus_wives:
                if isinstance(bw, list) and len(bw) > 0:
                    bonus_texts.append(self._format_wife_display(bw[0]))
                    bw_img = self.build_image_component(bw[0])
                    if bw_img:
                        chain.append(bw_img)
            if bonus_texts:
                text += f"\n🎁 纯爱奖励老婆：{'、'.join(bonus_texts)}"
                chain[0] = Plain(text)
        
        # 如果有牛来的老婆，也一起显示
        if ntr_wife_list:
            ntr_texts = []
            for ntr_wife in ntr_wife_list:
                if not isinstance(ntr_wife, list) or len(ntr_wife) < 1:
                    continue
                ntr_texts.append(self._format_wife_display(ntr_wife[0]))
                ntr_image = self.build_image_component(ntr_wife[0])
                if ntr_image:
                    chain.append(ntr_image)
            
            if ntr_texts:
                text += f"\n你还有{len(ntr_texts)}个牛来的老婆：{', '.join(ntr_texts)}~"
                chain[0] = Plain(text)
        
        try:
            yield event.chain_result(chain)
        except:
            yield event.plain_result(text)

    @filter.command("牛老婆")
    async def ntr_wife(self, event: AstrMessageEvent):
        """@某人尝试抢夺对方的老婆，有概率成功，每日限定次数
        
        纯爱模式影响：
        - 目标处于纯爱模式 → 拒绝（无法被牛）
        - 攻击者处于纯爱模式 → 暗中100%失败 + 概率导致尘白老婆跑路
        """
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        if not ntr_statuses.get(gid, True):
            yield event.plain_result("牛老婆功能还没开启哦，请联系管理员开启~")
            return
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        grp = ntr_records.setdefault(gid, {})
        rec = grp.get(uid, {"date": today, "count": 0})
        if rec["date"] != today:
            rec = {"date": today, "count": 0}
        if rec["count"] >= self.ntr_max:
            yield event.plain_result(
                f"{nick}，你今天已经牛了{self.ntr_max}次啦，明天再来吧~"
            )
            return
        tid = self.parse_target(event)
        if not tid or tid == uid:
            msg = "请@你想牛的对象哦~" if not tid else "不能牛自己呀，换个人试试吧~"
            yield event.plain_result(f"{nick}，{msg}")
            return
        
        cfg = load_group_config(gid)
        target_data = self._ensure_user_data(cfg, tid)
        drawn_wife = target_data.get("drawn")
        ntr_wife_list = target_data.get("ntr", [])
        if not isinstance(ntr_wife_list, list):
            ntr_wife_list = [ntr_wife_list] if ntr_wife_list else []
        
        # 获取被牛用户的昵称
        target_nick = None
        if drawn_wife:
            target_nick = drawn_wife[2]
        elif ntr_wife_list and len(ntr_wife_list) > 0:
            target_nick = ntr_wife_list[0][2] if isinstance(ntr_wife_list[0], list) and len(ntr_wife_list[0]) > 2 else None
        
        # 检查目标是否在CD中（使用被牛用户的昵称）
        in_cd, cd_msg, _ = check_ntr_cd(gid, tid, self.ntr_cd_duration, target_nick)
        if in_cd:
            yield event.plain_result(cd_msg)
            return
        
        # 检查是否有可牛的老婆
        can_ntr_drawn = drawn_wife and drawn_wife[1] == today
        can_ntr_ntr = len(ntr_wife_list) > 0
        
        if not can_ntr_drawn and not can_ntr_ntr:
            yield event.plain_result("对方今天还没有老婆可牛哦~")
            return
        
        # ===== 纯爱模式检查：目标受保护 =====
        target_pl = self._get_pure_love_info(target_data)
        if target_pl["active"]:
            yield event.plain_result("💕 对方处于纯爱模式，老婆受到纯爱守护，无法被牛哦~")
            return
        
        # 检查目标老婆是否受保护（管理员派发的）
        if target_data.get("protected", False):
            yield event.plain_result("对方的老婆是管理员派发的，受到特殊保护，无法被牛哦~")
            return
        
        # ===== 纯爱模式检查：攻击者处于纯爱模式 =====
        attacker_data = self._ensure_user_data(cfg, uid)
        attacker_pl = self._get_pure_love_info(attacker_data)
        
        if attacker_pl["active"]:
            # 纯爱用户去牛别人：暗中100%失败 + 概率跑路
            rec["count"] += 1
            grp[uid] = rec
            save_ntr_records()
            
            # 判断是否跑路
            if random.random() < self.pure_love_runaway_prob:
                # 尘白老婆跑路！
                old_wife = attacker_data.get("drawn")
                old_wife_display = self._format_wife_display(old_wife[0]) if old_wife else "你的纯爱老婆"
                self._handle_pure_love_runaway(attacker_data, today)
                save_group_config(gid, cfg)
                
                rem = self.ntr_max - rec["count"]
                yield event.plain_result(
                    f"💔 {nick}，牛老婆失败了……\n"
                    f"更糟糕的是，你的纯爱老婆 {old_wife_display} 发现你想牛别人，伤心地跑路了！\n"
                    f"纯爱模式已解除，今天无法再抽老婆了。你还可以再牛{rem}次~"
                )
                return
            else:
                # 没跑路，但牛还是失败了（暗中100%失败）
                rem = self.ntr_max - rec["count"]
                yield event.plain_result(
                    f"{nick}，很遗憾，牛失败了！你今天还可以再试{rem}次~"
                )
                return
        
        # ===== 正常牛老婆逻辑（非纯爱模式） =====
        # 如果有牛来的老婆，只能牛牛来的老婆（抽的原配不能牛）
        # 但有一定概率两个一起牛走
        ntr_both = False
        target_wife = None
        
        if can_ntr_ntr:
            # 有牛来的老婆，只能牛牛来的，但可能两个一起牛走
            if can_ntr_drawn and random.random() < self.ntr_both_probability:
                ntr_both = True
            # 随机选择一个牛来的老婆
            target_wife = random.choice(ntr_wife_list)
        else:
            # 只有抽的原配，可以牛
            target_wife = drawn_wife
        
        rec["count"] += 1
        grp[uid] = rec
        save_ntr_records()
        
        if random.random() < self.ntr_possibility:
            # 牛成功
            attacker_data = self._ensure_user_data(cfg, uid)
            if attacker_data.get("ntr") is None:
                attacker_data["ntr"] = []
            if not isinstance(attacker_data["ntr"], list):
                attacker_data["ntr"] = [attacker_data["ntr"]] if attacker_data["ntr"] else []
            
            if ntr_both:
                # 两个一起牛走
                # 将抽的原配作为ntr给牛的人（追加到列表）
                attacker_data["ntr"].append([drawn_wife[0], today, nick])
                # 将牛来的老婆也追加到列表
                if isinstance(target_wife, list):
                    attacker_data["ntr"].append([target_wife[0], today, nick])
                else:
                    attacker_data["ntr"].append([target_wife[0], today, nick])
                # 清除目标的老婆并设置CD
                cfg[tid] = {"drawn": None, "ntr": []}
                # 设置CD
                grp_cd = ntr_cd.setdefault(gid, {})
                grp_cd[tid] = {
                    "time": (datetime.utcnow() + timedelta(hours=8)).isoformat(),
                    "ntr_user_id": uid
                }
                save_ntr_cd()
                save_group_config(gid, cfg)
                # 检查并取消相关交换请求
                cancel_msg = await self.cancel_swap_on_wife_change(gid, [uid, tid])
                # 格式化CD时间
                hours = int(self.ntr_cd_duration // 3600)
                minutes = int((self.ntr_cd_duration % 3600) // 60)
                seconds = int(self.ntr_cd_duration % 60)
                if hours > 0:
                    cd_time_str = f"{hours}小时{minutes}分{seconds}秒"
                elif minutes > 0:
                    cd_time_str = f"{minutes}分{seconds}秒"
                else:
                    cd_time_str = f"{seconds}秒"
                yield event.plain_result(f"{nick}，牛老婆成功！你牛走了对方的两个老婆（抽的原配和牛来的），恭喜恭喜~苦主禁入{cd_time_str}的抽老婆CD")
                if cancel_msg:
                    yield event.plain_result(cancel_msg)
                
                # 如果牛的人（uid）在CD中，并且成功牛了别人，清除自己的CD
                if uid in ntr_cd.get(gid, {}):
                    # uid成功牛了别人，清除自己的CD
                    del ntr_cd[gid][uid]
                    if not ntr_cd[gid]:
                        del ntr_cd[gid]
                    save_ntr_cd()
            else:
                # 只牛走一个
                # 将牛来的老婆追加到列表（不替换）
                if isinstance(target_wife, list):
                    attacker_data["ntr"].append([target_wife[0], today, nick])
                else:
                    attacker_data["ntr"].append([target_wife[0], today, nick])
                
                # 清除目标被牛走的老婆
                if can_ntr_ntr:
                    # 牛走的是牛来的老婆，从列表中移除
                    if isinstance(target_wife, list):
                        ntr_wife_list = [w for w in ntr_wife_list if not (isinstance(w, list) and len(w) > 0 and w[0] == target_wife[0])]
                    else:
                        ntr_wife_list = [w for w in ntr_wife_list if not (isinstance(w, list) and len(w) > 0 and w[0] == target_wife[0])]
                    cfg[tid]["ntr"] = ntr_wife_list
                else:
                    # 牛走的是抽的原配
                    cfg[tid]["drawn"] = None
                
                # 如果目标两个老婆都没了，设置CD
                if (cfg[tid].get("drawn") is None or (cfg[tid].get("drawn") and cfg[tid]["drawn"][1] != today)) and (not cfg[tid].get("ntr") or len(cfg[tid]["ntr"]) == 0):
                    grp_cd = ntr_cd.setdefault(gid, {})
                    grp_cd[tid] = {
                        "time": (datetime.utcnow() + timedelta(hours=8)).isoformat(),
                        "ntr_user_id": uid
                    }
                    save_ntr_cd()
                
                save_group_config(gid, cfg)
                # 检查并取消相关交换请求
                cancel_msg = await self.cancel_swap_on_wife_change(gid, [uid, tid])
                wife_type = "牛来的老婆" if can_ntr_ntr else "抽的原配"
                # 格式化CD时间
                hours = int(self.ntr_cd_duration // 3600)
                minutes = int((self.ntr_cd_duration % 3600) // 60)
                seconds = int(self.ntr_cd_duration % 60)
                if hours > 0:
                    cd_time_str = f"{hours}小时{minutes}分{seconds}秒"
                elif minutes > 0:
                    cd_time_str = f"{minutes}分{seconds}秒"
                else:
                    cd_time_str = f"{seconds}秒"
                yield event.plain_result(f"{nick}，牛老婆成功！你牛走了对方的{wife_type}，恭喜恭喜~苦主禁入{cd_time_str}的抽老婆CD")
                if cancel_msg:
                    yield event.plain_result(cancel_msg)
            
            # 如果牛的人（uid）在CD中，并且成功牛了别人，清除自己的CD
            if uid in ntr_cd.get(gid, {}):
                # uid成功牛了别人，清除自己的CD
                del ntr_cd[gid][uid]
                if not ntr_cd[gid]:
                    del ntr_cd[gid]
                save_ntr_cd()
        else:
            rem = self.ntr_max - rec["count"]
            yield event.plain_result(
                f"{nick}，很遗憾，牛失败了！你今天还可以再试{rem}次~"
            )

    @filter.command("查老婆")
    async def search_wife(self, event: AstrMessageEvent):
        """查看自己或@某人的老婆信息，包括抽到的和牛来的"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        tid = self.parse_target(event) or str(event.get_sender_id())
        today = get_today()
        
        cfg = load_group_config(gid)
        # 清理过期的牛来的老婆记录
        clean_expired_ntr_records(gid, cfg, today)
        
        user_data = cfg.get(tid, {"drawn": None, "ntr": []})
        drawn_wife = user_data.get("drawn")
        ntr_wife_list = user_data.get("ntr", [])
        if not isinstance(ntr_wife_list, list):
            ntr_wife_list = [ntr_wife_list] if ntr_wife_list else []
        
        # 获取被查用户的昵称
        target_nick = None
        if drawn_wife:
            target_nick = drawn_wife[2]
        elif ntr_wife_list and len(ntr_wife_list) > 0:
            target_nick = ntr_wife_list[0][2] if isinstance(ntr_wife_list[0], list) and len(ntr_wife_list[0]) > 2 else "该用户"
        
        # 检查CD状态（无论查自己还是查别人）
        in_cd, cd_msg, _ = check_ntr_cd(gid, tid, self.ntr_cd_duration, target_nick)
        if in_cd:
            yield event.plain_result(cd_msg)
            return
        
        # 检查跑路状态
        pl_info = self._get_pure_love_info(user_data)
        if pl_info["runaway_date"] == today:
            owner = target_nick or "该用户"
            yield event.plain_result(f"💔 {owner}的纯爱老婆今天跑路了，当天无法再抽老婆。")
            # 但仍然可以显示牛来的老婆（如果有）
            if not ntr_wife_list:
                return
        
        # 如果既没有抽到的也没有牛来的，或者抽到的不是今天的（且非纯爱）
        has_drawn_today = drawn_wife is not None and (drawn_wife[1] == today or pl_info["active"])
        if not has_drawn_today and len(ntr_wife_list) == 0:
            yield event.plain_result("没有发现老婆的踪迹，快去抽一个试试吧~")
            return
        
        # 构建文本和图片链
        chain = []
        text_parts = []
        
        # 优先显示抽到的老婆
        if has_drawn_today:
            img = drawn_wife[0]
            name = os.path.splitext(img)[0]
            owner = drawn_wife[2]
            # 解析出处和角色名，分隔符为!
            if "!" in name:
                source, chara = name.split("!", 1)
                wife_text = f"{owner}的老婆是来自《{source}》的{chara}"
            else:
                wife_text = f"{owner}的老婆是{name}"
            
            if pl_info["active"]:
                bonus_avail = pl_info.get("bonus_available", 0)
                bonus_hint = " · 🎁有奖励" if bonus_avail > 0 else ""
                wife_text += f"（💕 纯爱模式 · 第{pl_info['days']}天{bonus_hint}）"
            wife_text += "，羡慕吗？"
            text_parts.append(wife_text)
            
            main_image = self.build_image_component(img)
            if main_image:
                chain.append(main_image)
        
        # 显示奖励老婆
        bonus_wives = user_data.get("pure_love_bonus_wives", [])
        if bonus_wives and isinstance(bonus_wives, list):
            bonus_texts = []
            for bw in bonus_wives:
                if isinstance(bw, list) and len(bw) > 0:
                    bonus_texts.append(self._format_wife_display(bw[0]))
                    bw_img = self.build_image_component(bw[0])
                    if bw_img:
                        chain.append(bw_img)
            if bonus_texts:
                text_parts.append(f"🎁 纯爱奖励老婆：{'、'.join(bonus_texts)}")
        
        # 显示所有牛来的老婆
        if ntr_wife_list:
            ntr_texts = []
            for ntr_wife in ntr_wife_list:
                if not isinstance(ntr_wife, list) or len(ntr_wife) < 1:
                    continue
                ntr_texts.append(self._format_wife_display(ntr_wife[0]))
                ntr_image = self.build_image_component(ntr_wife[0])
                if ntr_image:
                    chain.append(ntr_image)
            
            if ntr_texts:
                if has_drawn_today:
                    text_parts.append(f"还有{len(ntr_texts)}个牛来的老婆：{', '.join(ntr_texts)}~")
                else:
                    owner = ntr_wife_list[0][2] if isinstance(ntr_wife_list[0], list) and len(ntr_wife_list[0]) > 2 else "该用户"
                    text_parts.append(f"{owner}有{len(ntr_texts)}个牛来的老婆：{', '.join(ntr_texts)}，羡慕吗？")
        
        if not text_parts:
            yield event.plain_result("没有发现老婆的踪迹，快去抽一个试试吧~")
            return
        
        text = "\n".join(text_parts)
        chain.insert(0, Plain(text))
        
        try:
            yield event.chain_result(chain)
        except:
            yield event.plain_result(text)

    @filter.command("切换ntr开关状态")
    async def switch_ntr(self, event: AstrMessageEvent):
        """【管理员】开启或关闭本群的牛老婆功能"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        if uid not in self.admins:
            yield event.plain_result(f"{nick}，你没有权限操作哦~")
            return
        ntr_statuses[gid] = not ntr_statuses.get(gid, False)
        save_ntr_statuses()
        load_ntr_statuses()
        state = "开启" if ntr_statuses[gid] else "关闭"
        yield event.plain_result(f"{nick}，NTR已{state}")

    @filter.command("发老婆")
    async def give_wife(self, event: AstrMessageEvent):
        """【管理员】给指定用户派发一个受保护的老婆，无法被牛或交换"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        if uid not in self.admins:
            yield event.plain_result(f"{nick}，你没有权限操作哦~")
            return
        
        # 解析目标用户
        tid = self.parse_at_target(event)
        if not tid:
            yield event.plain_result("请在命令中@需要发老婆的对象哦~\n用法：发老婆 老婆名 @用户 或 发老婆 @用户 老婆名")
            return
        
        # 解析老婆名称（去掉@部分和命令本身）
        msg = event.message_str.strip()
        # 移除命令前缀
        if msg.startswith("发老婆"):
            msg = msg[3:].strip()
        
        # 从消息中提取老婆名（排除@部分）
        wife_name = ""
        parts = msg.split()
        for part in parts:
            # 跳过@相关的部分
            if not part.startswith("@") and part:
                wife_name = part
                break
        
        if not wife_name:
            yield event.plain_result("请指定老婆名称哦~\n用法：发老婆 老婆名 @用户 或 发老婆 @用户 老婆名")
            return
        
        # 查找匹配的老婆图片
        local_imgs = os.listdir(IMG_DIR)
        matched_img = None
        
        # 先尝试精确匹配（不含扩展名）
        for img in local_imgs:
            img_name = os.path.splitext(img)[0]
            if img_name == wife_name:
                matched_img = img
                break
        
        # 如果没有精确匹配，尝试模糊匹配
        if not matched_img:
            for img in local_imgs:
                img_name = os.path.splitext(img)[0]
                # 支持 "出处!角色名" 格式，匹配角色名部分
                if "!" in img_name:
                    source, chara = img_name.split("!", 1)
                    if wife_name in chara or wife_name in source or wife_name in img_name:
                        matched_img = img
                        break
                elif wife_name in img_name:
                    matched_img = img
                    break
        
        if not matched_img:
            yield event.plain_result(f"找不到名为「{wife_name}」的老婆图片哦~请检查老婆名称是否正确")
            return
        
        today = get_today()
        cfg = load_group_config(gid)
        
        # 获取目标用户昵称
        target_nick = None
        if tid in cfg:
            target_data = cfg[tid]
            if isinstance(target_data, dict):
                drawn = target_data.get("drawn")
                if drawn and len(drawn) > 2:
                    target_nick = drawn[2]
        if not target_nick:
            target_nick = f"用户{tid}"
        
        # 设置老婆，标记为受保护
        if tid not in cfg:
            cfg[tid] = {"drawn": None, "ntr": [], "protected": False}
        cfg[tid]["drawn"] = [matched_img, today, target_nick]
        cfg[tid]["protected"] = True  # 标记为管理员派发，受保护
        # 清除纯爱模式（管理员派发的走 protected 逻辑，不走纯爱逻辑）
        if cfg[tid].get("pure_love", False):
            self._clear_pure_love(cfg[tid])
            cfg[tid]["pure_love_bonus_wives"] = []
        save_group_config(gid, cfg)
        
        # 清除该用户的CD（如果有）
        grp_cd = ntr_cd.get(gid, {})
        if tid in grp_cd:
            del grp_cd[tid]
            if not grp_cd:
                ntr_cd.pop(gid, None)
            else:
                ntr_cd[gid] = grp_cd
            save_ntr_cd()
        
        # 取消相关交换请求
        cancel_msg = await self.cancel_swap_on_wife_change(gid, [tid])
        
        # 解析老婆显示名
        display_text = self._format_wife_display(matched_img)
        
        # 构建回复
        chain = [
            Plain(f"🎁 恭喜 "),
            At(qq=int(tid)),
            Plain(f" 获得管理员派发的老婆：{display_text}！\n该老婆受到特殊保护，无法被牛、无法交换，将保留至24点自动刷新~")
        ]
        
        # 添加图片
        img_component = self.build_image_component(matched_img)
        if img_component:
            chain.append(img_component)
        
        try:
            yield event.chain_result(chain)
        except:
            yield event.plain_result(f"🎁 恭喜用户获得管理员派发的老婆：{display_text}！该老婆受到特殊保护，无法被牛、无法交换~")
        
        if cancel_msg:
            yield event.plain_result(cancel_msg)

    @filter.command("拆散")
    async def breakup_wife(self, event: AstrMessageEvent):
        """【管理员】@某人强制清空其所有老婆，或指定拆散某个老婆
        
        用法：
        - 拆散 @用户 → 清空该用户所有老婆
        - 拆散 @用户 老婆名 → 只拆散指定的那个老婆
        """
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        if uid not in self.admins:
            yield event.plain_result(f"{nick}，你没有权限操作哦~")
            return
        tid = self.parse_at_target(event)
        if not tid:
            yield event.plain_result("请在拆散后@需要拆散的对象哦~\n用法：拆散 @用户 [老婆名]")
            return
        cfg = load_group_config(gid)
        target_data = cfg.get(tid)
        if not target_data or (
            (not target_data.get("drawn") or target_data["drawn"][0] is None)
            and (not target_data.get("ntr") or len(target_data.get("ntr")) == 0)
        ):
            yield event.plain_result("对方当前没有老婆需要拆散哦~")
            return
        
        # 解析是否指定了老婆名
        msg = event.message_str.strip()
        if msg.startswith("拆散"):
            msg = msg[2:].strip()
        # 从消息中提取老婆名（排除@部分）
        wife_keyword = ""
        parts = msg.split()
        for part in parts:
            if not part.startswith("@") and part:
                wife_keyword = part
                break
        
        target_name = target_data.get("drawn", [None, None, "该用户"])[2] if isinstance(target_data, dict) else "该用户"
        
        if wife_keyword:
            # 指定拆散某个老婆
            found = False
            removed_name = ""
            
            # 检查抽到的老婆是否匹配
            drawn = target_data.get("drawn")
            if drawn and drawn[0]:
                drawn_display = os.path.splitext(drawn[0])[0]
                # 支持 "出处!角色名" 格式匹配
                if wife_keyword in drawn_display:
                    cfg[tid]["drawn"] = None
                    found = True
                    if "!" in drawn_display:
                        _, chara = drawn_display.split("!", 1)
                        removed_name = chara
                    else:
                        removed_name = drawn_display
                    # 如果拆散的是纯爱老婆，清除纯爱状态
                    if target_data.get("pure_love", False):
                        self._clear_pure_love(target_data)
                        target_data["pure_love_bonus_wives"] = []
            
            # 如果抽到的没匹配，检查牛来的老婆列表
            if not found:
                ntr_list = target_data.get("ntr", [])
                new_ntr_list = []
                for ntr_wife in ntr_list:
                    if not isinstance(ntr_wife, list) or len(ntr_wife) < 1:
                        new_ntr_list.append(ntr_wife)
                        continue
                    ntr_display = os.path.splitext(ntr_wife[0])[0]
                    if wife_keyword in ntr_display and not found:
                        found = True
                        if "!" in ntr_display:
                            _, chara = ntr_display.split("!", 1)
                            removed_name = chara
                        else:
                            removed_name = ntr_display
                        # 跳过这个（不加入新列表 = 删除）
                    else:
                        new_ntr_list.append(ntr_wife)
                if found:
                    cfg[tid]["ntr"] = new_ntr_list
            
            # 如果还没找到，检查奖励老婆列表
            if not found:
                bonus_list = target_data.get("pure_love_bonus_wives", [])
                new_bonus_list = []
                for bw in bonus_list:
                    if not isinstance(bw, list) or len(bw) < 1:
                        new_bonus_list.append(bw)
                        continue
                    bw_display = os.path.splitext(bw[0])[0]
                    if wife_keyword in bw_display and not found:
                        found = True
                        if "!" in bw_display:
                            _, chara = bw_display.split("!", 1)
                            removed_name = chara
                        else:
                            removed_name = bw_display
                    else:
                        new_bonus_list.append(bw)
                if found:
                    cfg[tid]["pure_love_bonus_wives"] = new_bonus_list
            
            if not found:
                yield event.plain_result(f"未找到名为「{wife_keyword}」的老婆，请检查名称是否正确~")
                return
            
            # 如果全部老婆都没了，清除保护状态
            if (cfg[tid].get("drawn") is None) and (not cfg[tid].get("ntr") or len(cfg[tid].get("ntr")) == 0):
                cfg[tid]["protected"] = False
            
            save_group_config(gid, cfg)
            # 取消与其相关的交换请求
            cancel_msg = await self.cancel_swap_on_wife_change(gid, [tid])
            yield event.plain_result(f"{nick} 已拆散 {target_name} 的老婆「{removed_name}」。")
            if cancel_msg:
                yield event.plain_result(cancel_msg)
        else:
            # 清除全部老婆信息（包括保护状态和纯爱状态）
            cfg[tid] = {"drawn": None, "ntr": [], "protected": False,
                        "pure_love": False, "pure_love_start": "",
                        "pure_love_days": 0, "pure_love_runaway": "",
                        "pure_love_bonus_wives": [],
                        "pure_love_bonus_available": 0,
                        "pure_love_last_reward_day": 0}
            save_group_config(gid, cfg)
            # 清除CD记录
            grp_cd = ntr_cd.get(gid, {})
            if tid in grp_cd:
                del grp_cd[tid]
                if not grp_cd:
                    ntr_cd.pop(gid, None)
                save_ntr_cd()
            # 取消与其相关的交换请求
            cancel_msg = await self.cancel_swap_on_wife_change(gid, [tid])
            yield event.plain_result(f"{nick} 已强制遣散 {target_name} 的所有老婆。")
            if cancel_msg:
                yield event.plain_result(cancel_msg)

    @filter.command("解除保护")
    async def unprotect_wife(self, event: AstrMessageEvent):
        """【管理员】@某人解除其老婆的保护状态，不拆散老婆"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        if uid not in self.admins:
            yield event.plain_result(f"{nick}，你没有权限操作哦~")
            return
        tid = self.parse_at_target(event)
        if not tid:
            yield event.plain_result("请在命令后@需要解除保护的对象哦~\n用法：解除保护 @用户")
            return
        cfg = load_group_config(gid)
        target_data = cfg.get(tid)
        if not target_data:
            yield event.plain_result("对方当前没有任何老婆数据哦~")
            return
        
        has_protection = target_data.get("protected", False)
        has_pure_love = target_data.get("pure_love", False)
        
        if not has_protection and not has_pure_love:
            yield event.plain_result("对方的老婆当前没有受到保护哦~")
            return
        
        # 清除保护状态和纯爱状态
        cfg[tid]["protected"] = False
        if has_pure_love:
            self._clear_pure_love(cfg[tid])
            cfg[tid]["pure_love_bonus_wives"] = []
        save_group_config(gid, cfg)
        target_name = "该用户"
        drawn = target_data.get("drawn")
        if drawn and len(drawn) > 2:
            target_name = drawn[2]
        
        protection_types = []
        if has_protection:
            protection_types.append("管理员保护")
        if has_pure_love:
            protection_types.append("纯爱模式")
        yield event.plain_result(f"{nick} 已解除 {target_name} 的{'和'.join(protection_types)}状态，现在可以被牛或交换了~")

    @filter.command("换老婆")
    async def change_wife(self, event: AstrMessageEvent):
        """消耗换老婆次数重新抽取一个新老婆，每日限定次数"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        cfg = load_group_config(gid)
        user_data = cfg.get(uid, {"drawn": None, "ntr": None})
        drawn_wife = user_data.get("drawn")
        
        recs = change_records.setdefault(gid, {})
        rec = recs.get(uid, {"date": "", "count": 0})
        if rec["date"] == today and rec["count"] >= self.change_max_per_day:
            yield event.plain_result(
                f"{nick}，你今天已经换了{self.change_max_per_day}次老婆啦，明天再来吧~"
            )
            return
        if drawn_wife is None or drawn_wife[1] != today:
            # 纯爱模式下，即使日期不是今天也有老婆（因为跨天保持）
            pl_info = self._get_pure_love_info(user_data)
            if pl_info["active"] and drawn_wife is not None:
                yield event.plain_result(f"💕 {nick}，你的纯爱老婆受到守护，无法更换哦~想换老婆的话，先去牛别人试试？（可能会导致纯爱老婆跑路哦）")
                return
            yield event.plain_result(f"{nick}，你今天还没有抽到的老婆，先去抽一个再来换吧~")
            return
        
        # 检查是否受保护（管理员派发或纯爱模式）
        if user_data.get("protected", False):
            yield event.plain_result(f"{nick}，你的老婆是管理员派发的，受到特殊保护，无法更换哦~")
            return
        
        # 检查纯爱模式
        pl_info = self._get_pure_love_info(user_data)
        if pl_info["active"]:
            yield event.plain_result(f"💕 {nick}，你的纯爱老婆受到守护，无法更换哦~想换老婆的话，先去牛别人试试？（可能会导致纯爱老婆跑路哦）")
            return
        
        # 只清除抽到的老婆，保留牛来的
        if uid not in cfg:
            cfg[uid] = {"drawn": None, "ntr": None}
        cfg[uid]["drawn"] = None
        save_group_config(gid, cfg)
        
        if rec["date"] != today:
            rec = {"date": today, "count": 1}
        else:
            rec["count"] += 1
        recs[uid] = rec
        save_change_records()
        # 检查并取消相关交换请求
        cancel_msg = await self.cancel_swap_on_wife_change(gid, [uid])
        if cancel_msg:
            yield event.plain_result(cancel_msg)
        # 立即展示新老婆
        async for res in self.animewife(event):
            yield res

    @filter.command("重置牛")
    async def reset_ntr(self, event: AstrMessageEvent):
        """重置牛老婆次数，有失败禁言风险；管理员可@某人直接重置"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        if uid in self.admins:
            tid = self.parse_at_target(event) or uid
            if gid in ntr_records and tid in ntr_records[gid]:
                del ntr_records[gid][tid]
                save_ntr_records()
            chain = [
                Plain("管理员操作：已重置"),
                At(qq=int(tid)),
                Plain("的牛老婆次数。"),
            ]
            yield event.chain_result(chain)
            return
        reset_records = load_json(RESET_SHARED_FILE)
        grp = reset_records.setdefault(gid, {})
        rec = grp.get(uid, {"date": today, "count": 0})
        if rec.get("date") != today:
            rec = {"date": today, "count": 0}
        if rec["count"] >= self.reset_max_uses_per_day:
            yield event.plain_result(
                f"{nick}，你今天已经用完{self.reset_max_uses_per_day}次重置机会啦，明天再来吧~"
            )
            return
        rec["count"] += 1
        grp[uid] = rec
        save_json(RESET_SHARED_FILE, reset_records)
        tid = self.parse_at_target(event) or uid
        if random.random() < self.reset_success_rate:
            if gid in ntr_records and tid in ntr_records[gid]:
                del ntr_records[gid][tid]
                save_ntr_records()
            chain = [Plain("已重置"), At(qq=int(tid)), Plain("的牛老婆次数。")]
            yield event.chain_result(chain)
        else:
            try:
                await event.bot.set_group_ban(
                    group_id=int(gid),
                    user_id=int(uid),
                    duration=self.reset_mute_duration,
                )
            except:
                pass
            yield event.plain_result(
                f"{nick}，重置牛失败，被禁言{self.reset_mute_duration}秒，下次记得再接再厉哦~"
            )

    @filter.command("重置换")
    async def reset_change_wife(self, event: AstrMessageEvent):
        """重置换老婆次数，有失败禁言风险；管理员可@某人直接重置"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        nick = event.get_sender_name()
        today = get_today()
        if uid in self.admins:
            tid = self.parse_at_target(event) or uid
            grp = change_records.setdefault(gid, {})
            if tid in grp:
                del grp[tid]
                if not grp:
                    del change_records[gid]
                save_change_records()
            chain = [
                Plain("管理员操作：已重置"),
                At(qq=int(tid)),
                Plain("的换老婆次数。"),
            ]
            yield event.chain_result(chain)
            return
        reset_records = load_json(RESET_SHARED_FILE)
        grp = reset_records.setdefault(gid, {})
        rec = grp.get(uid, {"date": today, "count": 0})
        if rec.get("date") != today:
            rec = {"date": today, "count": 0}
        if rec["count"] >= self.reset_max_uses_per_day:
            yield event.plain_result(
                f"{nick}，你今天已经用完{self.reset_max_uses_per_day}次重置机会啦，明天再来吧~"
            )
            return
        rec["count"] += 1
        grp[uid] = rec
        save_json(RESET_SHARED_FILE, reset_records)
        tid = self.parse_at_target(event) or uid
        if random.random() < self.reset_success_rate:
            grp2 = change_records.setdefault(gid, {})
            if tid in grp2:
                del grp2[tid]
                if not grp2:
                    del change_records[gid]
                save_change_records()
            chain = [Plain("已重置"), At(qq=int(tid)), Plain("的换老婆次数。")]
            yield event.chain_result(chain)
        else:
            try:
                await event.bot.set_group_ban(
                    group_id=int(gid),
                    user_id=int(uid),
                    duration=self.reset_mute_duration,
                )
            except:
                pass
            yield event.plain_result(
                f"{nick}，重置换失败，被禁言{self.reset_mute_duration}秒，下次记得再接再厉哦~"
            )

    @filter.command("交换老婆")
    async def swap_wife(self, event: AstrMessageEvent):
        """@某人发起交换老婆请求，需对方同意后生效"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        uid = str(event.get_sender_id())
        tid = self.parse_at_target(event)
        nick = event.get_sender_name()
        today = get_today()
        grp_limit = swap_limit_records.setdefault(gid, {})
        rec_lim = grp_limit.get(uid, {"date": "", "count": 0})
        if rec_lim["date"] != today:
            rec_lim = {"date": today, "count": 0}
        if rec_lim["count"] >= self.swap_max_per_day:
            yield event.plain_result(
                f"{nick}，你今天已经发起了{self.swap_max_per_day}次交换请求啦，明天再来吧~"
            )
            return
        if not tid or tid == uid:
            yield event.plain_result(f"{nick}，请在命令后@你想交换的对象哦~")
            return
        cfg = load_group_config(gid)
        for x in (uid, tid):
            user_data = cfg.get(x, {"drawn": None, "ntr": None})
            drawn_wife = user_data.get("drawn")
            if drawn_wife is None or drawn_wife[1] != today:
                # 纯爱模式跨天也算有老婆
                pl = self._get_pure_love_info(user_data)
                if pl["active"] and drawn_wife is not None:
                    # 纯爱用户有老婆但受保护
                    who = "你" if x == uid else "对方"
                    yield event.plain_result(f"💕 {who}处于纯爱模式，老婆受到纯爱守护，无法交换哦~")
                    return
                who = nick if x == uid else "对方"
                yield event.plain_result(f"{who}，今天还没有抽到的老婆，无法进行交换哦~")
                return
            # 检查是否受保护
            if user_data.get("protected", False):
                who = "你" if x == uid else "对方"
                yield event.plain_result(f"{who}的老婆是管理员派发的，受到特殊保护，无法交换哦~")
                return
            # 检查纯爱模式
            pl = self._get_pure_love_info(user_data)
            if pl["active"]:
                who = "你" if x == uid else "对方"
                yield event.plain_result(f"💕 {who}处于纯爱模式，老婆受到纯爱守护，无法交换哦~")
                return
        rec_lim["count"] += 1
        grp_limit[uid] = rec_lim
        save_swap_limit_records()
        grp = swap_requests.setdefault(gid, {})
        grp[uid] = {"target": tid, "date": today}
        save_swap_requests()
        yield event.chain_result(
            [
                Plain(f"{nick} 想和 "),
                At(qq=int(tid)),
                Plain(
                    ' 交换老婆啦！请对方用"同意交换 @发起者"或"拒绝交换 @发起者"来回应~'
                ),
            ]
        )

    @filter.command("同意交换")
    async def agree_swap_wife(self, event: AstrMessageEvent):
        """@发起者同意其交换老婆的请求"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        tid = str(event.get_sender_id())
        uid = self.parse_at_target(event)
        nick = event.get_sender_name()
        grp = swap_requests.get(gid, {})
        rec = grp.get(uid)
        if not rec or rec.get("target") != tid:
            yield event.plain_result(
                f'{nick}，请在命令后@发起者，或用"查看交换请求"命令查看当前请求哦~'
            )
            return
        cfg = load_group_config(gid)
        user_u_data = cfg.get(uid, {"drawn": None, "ntr": None})
        user_t_data = cfg.get(tid, {"drawn": None, "ntr": None})
        drawn_u = user_u_data.get("drawn")
        drawn_t = user_t_data.get("drawn")
        
        # 交换抽到的老婆
        if drawn_u and drawn_t:
            # 交换老婆名称，保留日期和昵称
            temp_img = drawn_u[0]
            drawn_u[0] = drawn_t[0]
            drawn_t[0] = temp_img
            cfg[uid]["drawn"] = drawn_u
            cfg[tid]["drawn"] = drawn_t
        save_group_config(gid, cfg)
        del grp[uid]
        save_swap_requests()
        # 检查并取消相关交换请求
        cancel_msg = await self.cancel_swap_on_wife_change(gid, [uid, tid])
        yield event.plain_result("交换成功！你们的老婆已经互换啦，祝幸福~")
        if cancel_msg:
            yield event.plain_result(cancel_msg)

    @filter.command("拒绝交换")
    async def reject_swap_wife(self, event: AstrMessageEvent):
        """@发起者拒绝其交换老婆的请求"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        tid = str(event.get_sender_id())
        uid = self.parse_at_target(event)
        nick = event.get_sender_name()
        grp = swap_requests.get(gid, {})
        rec = grp.get(uid)
        if not rec or rec.get("target") != tid:
            yield event.plain_result(
                f'{nick}，请在命令后@发起者，或用"查看交换请求"命令查看当前请求哦~'
            )
            return
        del grp[uid]
        save_swap_requests()
        yield event.chain_result(
            [At(qq=int(uid)), Plain("，对方婉拒了你的交换请求，下次加油吧~")]
        )

    @filter.command("查看交换请求")
    async def view_swap_requests(self, event: AstrMessageEvent):
        """查看当前待处理的交换老婆请求列表"""
        if not self._check_group(event):
            yield event.plain_result("该功能仅限群聊使用哦~")
            return
        gid = str(event.message_obj.group_id)
        me = str(event.get_sender_id())
        today = get_today()
        grp = swap_requests.get(gid, {})
        cfg = load_group_config(gid)
        sent_targets = [rec["target"] for uid, rec in grp.items() if uid == me]
        received_from = [uid for uid, rec in grp.items() if rec.get("target") == me]
        if not sent_targets and not received_from:
            yield event.plain_result("你当前没有任何交换请求哦~")
            return
        parts = []
        for tid in sent_targets:
            user_data = cfg.get(tid, {"drawn": None, "ntr": []})
            drawn = user_data.get("drawn")
            ntr_list = user_data.get("ntr", [])
            if drawn:
                name = drawn[2]
            elif ntr_list and isinstance(ntr_list, list) and len(ntr_list) > 0:
                ntr_wife = ntr_list[0]
                name = ntr_wife[2] if isinstance(ntr_wife, list) and len(ntr_wife) > 2 else "未知用户"
            else:
                name = "未知用户"
            parts.append(f"→ 你发起给 {name} 的交换请求")
        for uid in received_from:
            user_data = cfg.get(uid, {"drawn": None, "ntr": []})
            drawn = user_data.get("drawn")
            ntr_list = user_data.get("ntr", [])
            if drawn:
                name = drawn[2]
            elif ntr_list and isinstance(ntr_list, list) and len(ntr_list) > 0:
                ntr_wife = ntr_list[0]
                name = ntr_wife[2] if isinstance(ntr_wife, list) and len(ntr_wife) > 2 else "未知用户"
            else:
                name = "未知用户"
            parts.append(f"→ {name} 发起给你的交换请求")
        text = (
            "当前交换请求如下：\n"
            + "\n".join(parts)
            + '\n请在"同意交换"或"拒绝交换"命令后@发起者进行操作~'
        )
        yield event.plain_result(text)

    async def cancel_swap_on_wife_change(self, gid, user_ids):
        # 检查并取消与user_ids相关的交换请求，返还交换次数，并返回提示文本（如有）。
        changed = False
        today = get_today()
        grp = swap_requests.get(gid, {})
        grp_limit = swap_limit_records.setdefault(gid, {})
        to_cancel = []
        for req_uid, req in grp.items():
            if req_uid in user_ids or req.get("target") in user_ids:
                to_cancel.append(req_uid)
        for req_uid in to_cancel:
            # 返还次数
            rec_lim = grp_limit.get(req_uid, {"date": "", "count": 0})
            if rec_lim.get("date") == today and rec_lim.get("count", 0) > 0:
                rec_lim["count"] = max(0, rec_lim["count"] - 1)
                grp_limit[req_uid] = rec_lim
                changed = True
            del grp[req_uid]
        if to_cancel:
            save_swap_requests()
        if changed:
            save_swap_limit_records()
        if to_cancel:
            return "检测到老婆变更，已自动取消相关交换请求并返还次数~"
        return None
