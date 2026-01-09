from astrbot.api.event import filter, AstrMessageEvent, MessageChain
from astrbot.api.star import Context, Star, register
from astrbot.api import logger
from astrbot.api.message_components import *
import json
import asyncio
import datetime
import aiohttp
import os
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple
import hashlib
import time

@register("CNKD_NEWS", "CNKD", "每日新闻图片插件", "0.1.0")
class CNKDNewsPlugin(Star):
    def __init__(self, context: Context, config: dict):
        super().__init__(context)
        self.config = config
        self.enabled = config.get("enabled", True)
        self.whitelist = set(config.get("whitelist", []))
        self.admin_users = set(config.get("admin_users", []))
        self.timezone = ZoneInfo(config.get("timezone", "Asia/Shanghai"))
        self.skip_weekend = config.get("skip_weekend", True)
        self.retry_times = config.get("retry_times", 3)
        self.timeout = config.get("timeout", 15)
        
        # API配置
        self.api_url = "https://uapis.cn/api/v1/daily/news-image"
        
        # 数据存储路径
        self.plugin_dir = Path(__file__).parent
        self.data_dir = self.plugin_dir / "data"
        self.data_dir.mkdir(exist_ok=True)
        
        # 定时任务存储
        self.schedule_file = self.data_dir / "schedules.json"
        self.cache_file = self.data_dir / "cache.json"
        
        # 定时任务字典：{group_umo: {"time": "HH:MM", "enabled": bool}}
        self.schedules: Dict[str, dict] = {}
        # 缓存：{"last_fetch": timestamp, "image_md5": str, "date": "YYYY-MM-DD"}
        self.cache: dict = {}
        
        # 加载数据
        self.load_data()
        
        # 启动定时任务检查
        asyncio.create_task(self.schedule_checker())
        
        logger.info(f"CNKD_NEWS插件已加载，白名单群组数: {len(self.whitelist)}")
    
    def load_data(self):
        """加载定时任务和缓存数据"""
        try:
            if self.schedule_file.exists():
                with open(self.schedule_file, 'r', encoding='utf-8') as f:
                    self.schedules = json.load(f)
            
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    self.cache = json.load(f)
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            self.schedules = {}
            self.cache = {}
    
    def save_data(self):
        """保存数据到文件"""
        try:
            with open(self.schedule_file, 'w', encoding='utf-8') as f:
                json.dump(self.schedules, f, ensure_ascii=False, indent=2)
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存数据失败: {e}")
    
    def is_admin(self, user_id: str) -> bool:
        """检查用户是否为管理员"""
        return user_id in self.admin_users
    
    def is_whitelisted(self, umo: str) -> bool:
        """检查群组是否在白名单中"""
        return umo in self.whitelist
    
    def parse_time(self, time_str: str) -> Optional[str]:
        """解析时间字符串，返回标准化格式 HH:MM"""
        try:
            # 支持格式: HH:MM, HHMM, H:MM
            if ':' in time_str:
                hour, minute = map(int, time_str.split(':'))
            elif len(time_str) == 4:
                hour, minute = int(time_str[:2]), int(time_str[2:])
            elif len(time_str) == 3:
                hour, minute = int(time_str[0]), int(time_str[1:])
            else:
                return None
            
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return f"{hour:02d}:{minute:02d}"
        except:
            pass
        return None
    
    def should_send_today(self) -> bool:
        """检查今天是否应该发送（考虑周末）"""
        if not self.skip_weekend:
            return True
        
        today = datetime.datetime.now(self.timezone).weekday()
        # 0-4: 周一到周五, 5-6: 周六周日
        return today <= 4
    
    async def fetch_news_image(self) -> Optional[bytes]:
        """获取每日新闻图片"""
        for attempt in range(self.retry_times):
            try:
                timeout = aiohttp.ClientTimeout(total=self.timeout)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(self.api_url) as response:
                        if response.status == 200:
                            content_type = response.headers.get('Content-Type', '')
                            if 'image/jpeg' in content_type or 'image/jpg' in content_type:
                                image_data = await response.read()
                                
                                # 计算MD5用于缓存
                                image_md5 = hashlib.md5(image_data).hexdigest()
                                today = datetime.datetime.now(self.timezone).strftime("%Y-%m-%d")
                                
                                # 更新缓存
                                self.cache = {
                                    "last_fetch": time.time(),
                                    "image_md5": image_md5,
                                    "date": today,
                                    "image_size": len(image_data)
                                }
                                self.save_data()
                                
                                logger.info(f"成功获取新闻图片，大小: {len(image_data)} 字节")
                                return image_data
                            else:
                                logger.warning(f"API返回非图片内容: {content_type}")
                        elif response.status == 500:
                            logger.error("API服务器内部错误")
                        elif response.status == 502:
                            logger.error("API网关错误，上游服务不可用")
                        else:
                            logger.error(f"API请求失败，状态码: {response.status}")
            except asyncio.TimeoutError:
                logger.warning(f"请求超时 ({attempt+1}/{self.retry_times})")
            except Exception as e:
                logger.error(f"获取新闻图片失败 ({attempt+1}/{self.retry_times}): {e}")
            
            if attempt < self.retry_times - 1:
                await asyncio.sleep(2)
        
        return None
    
    async def send_news_to_group(self, group_umo: str):
        """发送新闻到指定群组"""
        if not self.is_whitelisted(group_umo):
            logger.warning(f"群组 {group_umo} 不在白名单中")
            return False
        
        try:
            # 获取新闻图片
            image_data = await self.fetch_news_image()
            if not image_data:
                logger.error("获取新闻图片失败")
                return False
            
            # 保存为临时文件
            with tempfile.NamedTemporaryFile(suffix='.jpg', delete=False) as tmp:
                tmp.write(image_data)
                temp_path = tmp.name
            
            try:
                # 创建消息链
                now = datetime.datetime.now(self.timezone)
                date_str = now.strftime("%Y年%m月%d日")
                weekday = ["一", "二", "三", "四", "五", "六", "日"][now.weekday()]
                
                message = MessageChain([
                    Plain(f"📰 简报 ({date_str} 星期{weekday})\n"),
                    Plain("🌍 CNKD|UapiPro\n\n"),
                    Image.fromFileSystem(temp_path)
                ])
                
                # 发送消息
                await self.context.send_message(group_umo, message)
                logger.info(f"已发送新闻到群组: {group_umo}")
                return True
            finally:
                # 清理临时文件
                try:
                    os.unlink(temp_path)
                except:
                    pass
                
        except Exception as e:
            logger.error(f"发送新闻到群组 {group_umo} 失败: {e}")
            return False
    
    async def schedule_checker(self):
        """定时任务检查器"""
        logger.info("定时任务检查器已启动")
        
        while True:
            try:
                if not self.enabled:
                    await asyncio.sleep(60)
                    continue
                
                now = datetime.datetime.now(self.timezone)
                current_time = now.strftime("%H:%M")
                current_date = now.strftime("%Y-%m-%d")
                
                # 检查今天是否应该发送
                if self.skip_weekend and now.weekday() >= 5:
                    await asyncio.sleep(60)
                    continue
                
                # 检查所有定时任务
                for group_umo, schedule in self.schedules.items():
                    if not schedule.get("enabled", True):
                        continue
                    
                    schedule_time = schedule.get("time")
                    last_sent = schedule.get("last_sent")
                    
                    if schedule_time == current_time and last_sent != current_date:
                        logger.info(f"触发定时任务: {group_umo} @ {current_time}")
                        
                        # 发送新闻
                        success = await self.send_news_to_group(group_umo)
                        
                        if success:
                            # 更新最后发送时间
                            self.schedules[group_umo]["last_sent"] = current_date
                            self.save_data()
                
                await asyncio.sleep(60)  # 每分钟检查一次
                
            except Exception as e:
                logger.error(f"定时任务检查器出错: {e}")
                await asyncio.sleep(60)
    
    # ========== 命令处理器 ==========
    
    @filter.command("新闻帮助")
    async def news_help(self, event: AstrMessageEvent):
        """显示插件帮助信息"""
        help_text = """📰 每日新闻插件使用帮助 📰

基础命令：
/新闻帮助 - 显示此帮助信息
/新闻 - 立即获取并发送今日新闻
/新闻状态 - 查看插件状态和定时任务

定时任务命令：
/新闻定时 HH:MM - 设置本群定时发送时间
/新闻定时关闭 - 关闭本群定时发送
/新闻定时列表 - 查看所有定时任务

管理员命令：
/新闻白名单添加 [UMO] - 添加群组到白名单
/新闻白名单删除 [UMO] - 从白名单移除群组
/新闻白名单列表 - 查看所有白名单群组
/新闻管理员添加 [UID] - 添加管理员
/新闻管理员删除 [UID] - 移除管理员

仓库地址：https://github.com/YiChex/astrbot_plugin_CNKD_NEWS

时间格式：HH:MM (24小时制)
示例：/新闻定时 09:00
        """
        yield event.plain_result(help_text)
    
    @filter.command("新闻")
    async def news_now(self, event: AstrMessageEvent):
        """立即获取并发送今日新闻"""
        group_umo = event.unified_msg_origin
        
        if not self.is_whitelisted(group_umo):
            yield event.plain_result("❌ 本群未在白名单中，无法使用此功能")
            return
        
        yield event.plain_result("⏳ 正在获取今日新闻，请稍候...")
        
        success = await self.send_news_to_group(group_umo)
        if success:
            yield event.plain_result("✅ 今日新闻发送成功！")
        else:
            yield event.plain_result("❌ 获取新闻失败，请稍后再试")
    
    @filter.command("新闻状态")
    async def news_status(self, event: AstrMessageEvent):
        """查看插件状态"""
        now = datetime.datetime.now(self.timezone)
        date_str = now.strftime("%Y-%m-%d %H:%M:%S")
        
        status_lines = [
            f"📊 每日新闻插件状态报告",
            f"时间: {date_str} (时区: {self.timezone})",
            f"插件状态: {'✅ 已启用' if self.enabled else '❌ 已禁用'}",
            f"白名单群组数: {len(self.whitelist)}",
            f"定时任务数: {len(self.schedules)}",
            f"周末跳过: {'是' if self.skip_weekend else '否'}",
            f"",
            f"本群状态:",
            f"  UMO: {event.unified_msg_origin}",
            f"  白名单: {'✅ 在白名单中' if self.is_whitelisted(event.unified_msg_origin) else '❌ 不在白名单中'}",
            f"  定时任务: {'✅ 已设置' if event.unified_msg_origin in self.schedules else '❌ 未设置'}"
        ]
        
        if event.unified_msg_origin in self.schedules:
            schedule = self.schedules[event.unified_msg_origin]
            status_lines.append(f"  发送时间: {schedule.get('time', '未设置')}")
            status_lines.append(f"  任务状态: {'✅ 已启用' if schedule.get('enabled', True) else '❌ 已禁用'}")
            if 'last_sent' in schedule:
                status_lines.append(f"  最后发送: {schedule['last_sent']}")
        
        yield event.plain_result("\n".join(status_lines))
    
    @filter.command("新闻定时")
    async def news_schedule(self, event: AstrMessageEvent, time_str: str = None):
        """设置或管理定时任务"""
        group_umo = event.unified_msg_origin
        
        if not self.is_whitelisted(group_umo):
            yield event.plain_result("❌ 本群未在白名单中，无法设置定时任务")
            return
        
        if time_str is None:
            # 显示当前设置
            if group_umo in self.schedules:
                schedule = self.schedules[group_umo]
                result = f"📅 本群定时任务设置:\n"
                result += f"发送时间: {schedule.get('time')}\n"
                result += f"任务状态: {'✅ 已启用' if schedule.get('enabled', True) else '❌ 已禁用'}\n"
                if 'last_sent' in schedule:
                    result += f"最后发送: {schedule['last_sent']}\n"
                result += f"\n使用 /新闻定时 HH:MM 修改时间\n"
                result += f"使用 /新闻定时关闭 关闭定时"
            else:
                result = "❌ 本群尚未设置定时任务\n"
                result += f"使用 /新闻定时 HH:MM 设置定时发送时间\n"
                result += f"示例: /新闻定时 09:00"
            yield event.plain_result(result)
            return
        
        if time_str.lower() in ["关闭", "off", "stop"]:
            if group_umo in self.schedules:
                self.schedules[group_umo]["enabled"] = False
                self.save_data()
                yield event.plain_result("✅ 已关闭本群定时新闻发送")
            else:
                yield event.plain_result("❌ 本群尚未设置定时任务")
            return
        
        if time_str.lower() in ["开启", "on", "start"]:
            if group_umo in self.schedules:
                self.schedules[group_umo]["enabled"] = True
                self.save_data()
                yield event.plain_result("✅ 已开启本群定时新闻发送")
            else:
                yield event.plain_result("❌ 本群尚未设置定时任务，请先设置时间")
            return
        
        # 设置时间
        parsed_time = self.parse_time(time_str)
        if not parsed_time:
            yield event.plain_result("❌ 时间格式错误，请使用 HH:MM 格式\n示例: /新闻定时 09:00")
            return
        
        if group_umo not in self.schedules:
            self.schedules[group_umo] = {}
        
        self.schedules[group_umo].update({
            "time": parsed_time,
            "enabled": True,
            "last_sent": None
        })
        self.save_data()
        
        yield event.plain_result(f"✅ 已设置本群定时新闻发送时间为 {parsed_time}\n每天 {parsed_time} 将自动发送每日新闻")
    
    @filter.command("新闻定时列表")
    async def news_schedule_list(self, event: AstrMessageEvent):
        """查看所有定时任务"""
        if not self.schedules:
            yield event.plain_result("📋 当前没有定时任务")
            return
        
        lines = ["📋 定时任务列表:"]
        for i, (group_umo, schedule) in enumerate(self.schedules.items(), 1):
            status = "✅" if schedule.get("enabled", True) else "❌"
            time_str = schedule.get("time", "未设置")
            last_sent = schedule.get("last_sent", "从未发送")
            lines.append(f"{i}. {group_umo}")
            lines.append(f"   时间: {time_str} {status}")
            lines.append(f"   最后发送: {last_sent}")
        
        yield event.plain_result("\n".join(lines))
    
    # ========== 管理员命令 ==========
    
    @filter.command("新闻白名单添加")
    async def news_whitelist_add(self, event: AstrMessageEvent, umo: str = None):
        """添加群组到白名单"""
        user_id = event.get_sender_id()
        if not self.is_admin(user_id):
            yield event.plain_result("❌ 只有管理员可以使用此命令")
            return
        
        if not umo:
            yield event.plain_result("❌ 请提供群组UMO\n格式: 平台:消息类型:会话ID\n示例: QQ:GroupMessage:123456789")
            return
        
        if umo in self.whitelist:
            yield event.plain_result(f"✅ 群组 {umo} 已在白名单中")
            return
        
        self.whitelist.add(umo)
        self.config["whitelist"] = list(self.whitelist)
        self.config.save_config()
        
        yield event.plain_result(f"✅ 已添加群组 {umo} 到白名单")
    
    @filter.command("新闻白名单删除")
    async def news_whitelist_remove(self, event: AstrMessageEvent, umo: str = None):
        """从白名单移除群组"""
        user_id = event.get_sender_id()
        if not self.is_admin(user_id):
            yield event.plain_result("❌ 只有管理员可以使用此命令")
            return
        
        if not umo:
            yield event.plain_result("❌ 请提供群组UMO")
            return
        
        if umo not in self.whitelist:
            yield event.plain_result(f"❌ 群组 {umo} 不在白名单中")
            return
        
        self.whitelist.remove(umo)
        self.config["whitelist"] = list(self.whitelist)
        self.config.save_config()
        
        # 同时删除定时任务
        if umo in self.schedules:
            del self.schedules[umo]
            self.save_data()
        
        yield event.plain_result(f"✅ 已从白名单移除群组 {umo}")
    
    @filter.command("新闻白名单列表")
    async def news_whitelist_list(self, event: AstrMessageEvent):
        """查看白名单群组"""
        user_id = event.get_sender_id()
        if not self.is_admin(user_id):
            yield event.plain_result("❌ 只有管理员可以使用此命令")
            return
        
        if not self.whitelist:
            yield event.plain_result("📋 白名单为空")
            return
        
        lines = ["📋 白名单群组列表:"]
        for i, umo in enumerate(sorted(self.whitelist), 1):
            has_schedule = "✅" if umo in self.schedules else "❌"
            lines.append(f"{i}. {umo} [定时任务: {has_schedule}]")
        
        lines.append(f"\n总计: {len(self.whitelist)} 个群组")
        yield event.plain_result("\n".join(lines))
    
    @filter.command("新闻管理员添加")
    async def news_admin_add(self, event: AstrMessageEvent, user_id: str = None):
        """添加管理员"""
        sender_id = event.get_sender_id()
        if not self.is_admin(sender_id):
            yield event.plain_result("❌ 只有管理员可以使用此命令")
            return
        
        if not user_id:
            yield event.plain_result("❌ 请提供用户ID")
            return
        
        if user_id in self.admin_users:
            yield event.plain_result(f"✅ 用户 {user_id} 已是管理员")
            return
        
        self.admin_users.add(user_id)
        self.config["admin_users"] = list(self.admin_users)
        self.config.save_config()
        
        yield event.plain_result(f"✅ 已添加用户 {user_id} 为管理员")
    
    @filter.command("新闻管理员删除")
    async def news_admin_remove(self, event: AstrMessageEvent, user_id: str = None):
        """移除管理员"""
        sender_id = event.get_sender_id()
        if not self.is_admin(sender_id):
            yield event.plain_result("❌ 只有管理员可以使用此命令")
            return
        
        if not user_id:
            yield event.plain_result("❌ 请提供用户ID")
            return
        
        if user_id not in self.admin_users:
            yield event.plain_result(f"❌ 用户 {user_id} 不是管理员")
            return
        
        if len(self.admin_users) <= 1:
            yield event.plain_result("❌ 至少需要保留一名管理员")
            return
        
        self.admin_users.remove(user_id)
        self.config["admin_users"] = list(self.admin_users)
        self.config.save_config()
        
        yield event.plain_result(f"✅ 已移除用户 {user_id} 的管理员权限")
    
    @filter.command("新闻缓存清理")
    async def news_clear_cache(self, event: AstrMessageEvent):
        """清理缓存"""
        user_id = event.get_sender_id()
        if not self.is_admin(user_id):
            yield event.plain_result("❌ 只有管理员可以使用此命令")
            return
        
        self.cache = {}
        self.save_data()
        
        yield event.plain_result("✅ 已清理新闻缓存")
    
    async def terminate(self):
        """插件卸载时的清理工作"""
        logger.info("CNKD_NEWS插件正在卸载...")
        # 保存数据
        self.save_data()
        logger.info("CNKD_NEWS插件已卸载")
