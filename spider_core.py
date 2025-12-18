import requests
import json
import pandas as pd
import os
from datetime import datetime, timedelta
import time
import logging
import sys

# 配置日志 - 修复语法错误
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bidding_crawler.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

class JnkgBiddingSpider:
    def __init__(self):
        self.base_url = "https://dzzb.jnkgjtdzzbgs.com"
        self.api_url = f"{self.base_url}/cms/api/dynamicData/queryContentPage"
        
        # 定义多个网站的配置
        self.website_configs = [
            {
                "name": "3ywgg1",
                "url": "/cms/default/webfile/3ywgg1/index.html",
                "site_id": "725",
                "category_id": "238"
            },
            {
                "name": "2ywgg1", 
                "url": "/cms/default/webfile/2ywgg1/index.html",
                "site_id": "725",  # 需要根据实际情况调整
                "category_id": "230"  # 需要根据实际情况调整
            },
            {
                "name": "1ywgg1",
                "url": "/cms/default/webfile/1ywgg1/index.html",
                "site_id": "725",  # 需要根据实际情况调整
                "category_id": "222"  # 需要根据实际情况调整
            }
        ]
        
        # 如果不知道其他网站的site_id和category_id，可以先使用默认值
        # 默认使用第一个网站的配置作为fallback
        self.default_site_id = self.website_configs[0]["site_id"]
        self.default_category_id = self.website_configs[0]["category_id"]
        
        self.page_size = 20
        
        # 搜索关键词
        self.keywords = ["天安","晋圣","晋煤"]
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json; charset=utf-8',
            'Origin': self.base_url,
        }
        
        # ============【在此处添加代理配置】============
        # 代理配置
        self.proxy_config = {
            'http': 'http://117.69.236.166:8089',
            'https': 'http://117.69.236.166:8089'
        }
        
        # 检查是否在GitHub Actions环境
        self.is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
        self.use_proxy = self.is_github_actions  # 在GitHub Actions中自动使用代理
        
        if self.use_proxy:
            print("🌐 检测到GitHub Actions环境，启用代理")
            print(f"🔗 代理地址: {self.proxy_config['http']}")
        # ============【代理配置结束】============
        
        # 显示当前工作目录
        print(f"📂 当前工作目录: {os.getcwd()}")
        print(f"📂 输出文件将保存在此目录")
        print("="*60)
    
    # 注意：search_by_keyword 方法应该与 __init__ 方法同级，不是内部方法
    def search_by_keyword(self, keyword, search_field="title", days_limit=10, site_id=None, category_id=None, referer_url=None):
        """按关键词搜索特定网站"""
        all_data = []
        page_no = 1
        
        # 使用参数或默认值
        site_id = site_id or self.default_site_id
        category_id = category_id or self.default_category_id
        
        # 设置Referer头
        headers = self.headers.copy()
        if referer_url:
            headers['Referer'] = f"{self.base_url}{referer_url}"
        else:
            headers['Referer'] = f"{self.base_url}/cms/default/webfile/3ywgg1/index.html"
        
        # 计算日期范围
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_limit)
        
        while True:
            try:
                payload = {
                    "pageNo": page_no,
                    "pageSize": self.page_size,
                    "dto": {
                        "siteId": site_id,
                        "categoryId": category_id,
                        "beginDate": start_date.strftime("%Y-%m-%d"),
                        "endDate": end_date.strftime("%Y-%m-%d"),
                    }
                }
                
                if search_field == "title":
                    payload["dto"]["title"] = keyword
                elif search_field == "agentCompanyName":
                    payload["dto"]["agentCompanyName"] = keyword
                
                # 构建基础请求参数
                request_params = {
                    'url': self.api_url,
                    'headers': headers,
                    'data': json.dumps(payload, ensure_ascii=False).encode('utf-8'),
                    'timeout': 30
                }
                
                # 仅当需要代理时，才添加 proxies 参数
                if self.use_proxy:
                    request_params['proxies'] = self.proxy_config
                    if page_no == 1:
                        print(f"📡 使用代理请求: {self.proxy_config['http']}")
                
                response = requests.post(**request_params)
                
                if response.status_code != 200:
                    logger.error(f"HTTP {response.status_code}: 请求失败")
                    print(f"❌ 请求失败，状态码: {response.status_code}")
                    break
                
                data = response.json()
                rows = data['res'].get('rows', [])
                total = data['res'].get('total', 0)
                
                if page_no == 1:
                    logger.info(f"网站配置[site_id={site_id}, category_id={category_id}] - 总共找到 {total} 条相关记录")
                    print(f"✅ 请求成功，找到 {total} 条相关记录")
                
                if not rows:
                    break
                
                all_data.extend(rows)
                
                if len(rows) < self.page_size:
                    break
                    
                page_no += 1
                time.sleep(1)  # 增加延迟，避免请求过快
                
            except requests.exceptions.ProxyError as e:
                logger.error(f"代理连接失败: {e}")
                print(f"❌ 代理连接失败: {e}")
                print("尝试使用备用代理或直接连接...")
                break
            except requests.exceptions.ConnectionError as e:
                logger.error(f"连接错误: {e}")
                print(f"❌ 连接错误: {e}")
                break
            except Exception as e:
                logger.error(f"搜索异常: {e}")
                print(f"❌ 搜索异常: {e}")
                break
        
        return all_data
    
    def search_website(self, website_config, days_limit=10):
        """搜索单个网站的所有关键词"""
        website_results = []
        website_name = website_config["name"]
        
        logger.info(f"\n{'='*60}")
        logger.info(f"开始爬取网站: {website_name}")
        logger.info(f"网站URL: {website_config['url']}")
        # 修复：删除多余的 logger. 和重复的变量名
        logger.info(f"配置: site_id={website_config['site_id']}, category_id={website_config['category_id']}")
        
        for keyword in self.keywords:
            logger.info(f"处理关键词: {keyword}")
            
            # 在标题中搜索
            title_results = self.search_by_keyword(
                keyword, "title", days_limit,
                website_config["site_id"], website_config["category_id"],
                website_config["url"]
            )
            time.sleep(1)
            
            # 在采购单位中搜索
            company_results = self.search_by_keyword(
                keyword, "agentCompanyName", days_limit,
                website_config["site_id"], website_config["category_id"],
                website_config["url"]
            )
            
            # 合并结果
            keyword_results = title_results + company_results
            
            # 去重
            seen = set()
            unique_results = []
            for item in keyword_results:
                item_id = f"{item.get('title', '')}_{item.get('publishDate', '')}"
                if item_id not in seen:
                    seen.add(item_id)
                    unique_results.append(item)
            
            logger.info(f"关键词 '{keyword}' 在网站 '{website_name}' 去重后得到 {len(unique_results)} 条唯一数据")
            
            # 提取数据
            for item in unique_results:
                extracted = self.extract_item_fields(item)
                extracted['搜索关键词'] = keyword
                extracted['来源网站'] = website_name
                extracted['网站URL'] = f"{self.base_url}{website_config['url']}"
                website_results.append(extracted)
        
        logger.info(f"网站 '{website_name}' 总计爬取 {len(website_results)} 条数据")
        return website_results
    
    # 保持与旧代码兼容的方法
    def search_all_keywords(self, days_limit=10):
        """兼容旧版本的搜索方法（只搜索第一个网站）"""
        logger.info("使用兼容模式：只搜索第一个网站")
        website_data = self.search_website(self.website_configs[0], days_limit)
        
        # 移除网站相关字段以保持与旧版本的兼容性
        clean_data = []
        for item in website_data:
            clean_item = item.copy()
            if '来源网站' in clean_item:
                del clean_item['来源网站']
            if '网站URL' in clean_item:
                del clean_item['网站URL']
            clean_data.append(clean_item)
                
        return clean_data
    
    def search_all_websites(self, days_limit=10):
        """搜索所有网站的关键词（新方法）"""
        all_results = []
        
        print(f"\n{'='*60}")
        print("🚀 开始爬取所有网站")
        print(f"搜索关键词: {self.keywords}")
        print(f"时间范围: 最近{days_limit}天")
        print(f"网站数量: {len(self.website_configs)}个")
        if self.use_proxy:
            print(f"📡 使用代理: {self.proxy_config['http']}")
        print(f"{'='*60}\n")
        
        for config in self.website_configs:
            try:
                print(f"🌐 正在爬取网站: {config['name']}")
                
                # 爬取当前网站
                website_data = self.search_website(config, days_limit)
                
                # 移除网站相关字段
                clean_data = []
                for item in website_data:
                    # 创建副本，避免修改原数据
                    clean_item = item.copy()
                    if '来源网站' in clean_item:
                        del clean_item['来源网站']
                    if '网站URL' in clean_item:
                        del clean_item['网站URL']
                    clean_data.append(clean_item)
                
                print(f"✅ 网站 '{config['name']}' 爬取完成: {len(clean_data)} 条数据")
                all_results.extend(clean_data)
                
                # 网站间延迟
                if config != self.website_configs[-1]:  # 不是最后一个网站
                    time.sleep(2)
                
            except Exception as e:
                print(f"❌ 爬取网站 {config['name']} 时出错: {e}")
                continue
        
        # 跨网站去重
        if all_results:
            seen = set()
            unique_results = []
            for item in all_results:
                # 使用标题+发布时间作为唯一标识
                item_id = f"{item.get('标题', '')}_{item.get('发布时间', '')}"
                if item_id not in seen:
                    seen.add(item_id)
                    unique_results.append(item)
            
            print(f"\n📊 所有网站爬取完成")
            print(f"原始数据: {len(all_results)} 条")
            print(f"去重后: {len(unique_results)} 条")
            print(f"{'='*60}")
        
        return unique_results if all_results else []
    
    def extract_item_fields(self, item):
        """提取数据字段"""
        publish_date = item.get('publishDate', '')
        if publish_date and 'T' in publish_date:
            publish_date = publish_date.split('T')[0]
        
        def format_date(date_str):
            if date_str and 'T' in date_str:
                return date_str.split('T')[0]
            return date_str or ''
        
        return {
            '标题': item.get('title', ''),
            '发布时间': publish_date,
            '采购单位': item.get('agentCompanyName', ''),
            '项目编号': item.get('mainCode', ''),
            '采购方式': item.get('purchaseModeName', item.get('purchaseMode', '')),
            '省份': item.get('provinceName', ''),
            '城市': item.get('cityName', ''),
            '分类': item.get('categoryName', ''),
            '链接': f"{self.base_url}{item.get('url', '')}" if item.get('url') else '',
            '详细内容': (item.get('text', '')[:100] + '...') if item.get('text') else '',
        }
    
    def save_results(self, data):
        """保存结果"""
        if not data:
            print("⚠️  没有数据可保存")
            return None
        
        df = pd.DataFrame(data)
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"晋能控股招标_{timestamp}"
        
        try:
            # 保存Excel
            excel_file = f"{filename}.xlsx"
            df.to_excel(excel_file, index=False, engine='openpyxl')
            print(f"\n✅ 数据已保存到Excel:")
            print(f"📁 文件位置: {os.path.abspath(excel_file)}")
            
            # 保存CSV
            csv_file = f"{filename}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"📁 文件位置: {os.path.abspath(csv_file)}")
            
        except Exception as e:
            print(f"保存Excel失败: {e}")
            # 只保存CSV
            csv_file = f"{filename}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"✅ 数据已保存到CSV: {os.path.abspath(csv_file)}")
        
        # 显示统计
        print(f"\n📊 统计结果:")
        print(f"总计数据: {len(df)} 条")
        print(f"时间范围: {df['发布时间'].min()} 至 {df['发布时间'].max()}")
        
        return df
    
    def run(self):
        """运行爬虫（多网站版）"""
        print("🚀 启动晋能控股招标数据爬虫（多网站版）")
        print(f"搜索关键词: {self.keywords}")
        print(f"爬取网站数: {len(self.website_configs)} 个")
        print(f"时间范围: 最近10天")
        
        all_data = []
        
        for config in self.website_configs:
            try:
                # 如果配置中没有site_id或category_id，尝试发现
                if "site_id" not in config or "category_id" not in config:
                    logger.warning(f"网站 {config['name']} 缺少配置参数，尝试使用默认值")
                    config["site_id"] = self.default_site_id
                    config["category_id"] = self.default_category_id
                
                # 爬取当前网站
                website_data = self.search_website(config, days_limit=10)
                all_data.extend(website_data)
                
                # 网站间延迟，避免请求过于频繁
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"爬取网站 {config['name']} 时出错: {e}")
                continue
        
        if not all_data:
            print("⚠️  所有网站均未找到符合条件的数据")
            return
        
        # 最终去重（跨网站去重）
        seen = set()
        unique_data = []
        for item in all_data:
            # 使用标题+发布时间+来源网站作为唯一标识
            item_id = f"{item.get('标题', '')}_{item.get('发布时间', '')}_{item.get('来源网站', '')}"
            if item_id not in seen:
                seen.add(item_id)
                unique_data.append(item)
        
        logger.info(f"跨网站去重后总计 {len(unique_data)} 条唯一数据")
        
        # 保存结果
        if '来源网站' in unique_data[0]:
            # 如果有来源网站字段，使用增强版保存
            self.save_results_enhanced(unique_data)
        else:
            # 否则使用普通保存
            self.save_results(unique_data)
        
        print(f"\n🎉 爬虫执行完成！")
    
    def save_results_enhanced(self, data):
        """增强版保存结果（包含多网站信息）"""
        if not data:
            print("⚠️  没有数据可保存")
            return None
        
        df = pd.DataFrame(data)
        
        # 按来源网站分组统计
        website_stats = df['来源网站'].value_counts()
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"晋能控股招标_多网站_{timestamp}"
        
        try:
            # 保存Excel
            excel_file = f"{filename}.xlsx"
            
            # 使用ExcelWriter创建多个sheet
            with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                # 主表：所有数据
                df.to_excel(writer, sheet_name='所有数据', index=False)
                
                # 按网站分表
                for website in df['来源网站'].unique():
                    website_df = df[df['来源网站'] == website]
                    # 简化sheet名（Excel sheet名最多31字符）
                    sheet_name = f"{website}"[:31]
                    website_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # 统计数据表
                stats_df = pd.DataFrame({
                    '网站': website_stats.index,
                    '数据量': website_stats.values
                })
                stats_df.to_excel(writer, sheet_name='统计', index=False)
            
            print(f"\n✅ 数据已保存到Excel:")
            print(f"📁 文件位置: {os.path.abspath(excel_file)}")
            
            # 保存CSV
            csv_file = f"{filename}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"📁 文件位置: {os.path.abspath(csv_file)}")
            
        except Exception as e:
            print(f"保存Excel失败: {e}")
            # 只保存CSV
            csv_file = f"{filename}.csv"
            df.to_csv(csv_file, index=False, encoding='utf-8-sig')
            print(f"✅ 数据已保存到CSV: {os.path.abspath(csv_file)}")
        
        # 显示统计
        print(f"\n📊 统计结果:")
        print(f"总计数据: {len(df)} 条")
        for website, count in website_stats.items():
            print(f"  - {website}: {count} 条")
        print(f"时间范围: {df['发布时间'].min()} 至 {df['发布时间'].max()}")

def main():
    spider = JnkgBiddingSpider()
    spider.run()

if __name__ == "__main__":
    main()
