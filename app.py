import datetime
import os
from optparse import OptionParser
import glob
import http.client
import urllib
import zipfile
from io import BytesIO
from xml.dom.minidom import getDOMImplementation
from xml.etree import cElementTree
import re
import time
import logging
import traceback
import tvdb_api



def log():
    return logging.getLogger("epgData2XMLTV")


class App:
    INPUT_PATH = 'epgdata_files'
    OUTPUT_PATH = 'output'
    APIKEY = '################'
    PIN = '######################'
    DAY = -1
    URL = 'www.epgdata.com'
    DATEFORMAT = '%Y%m%d'
    timeoffset ="+0000"

    channel_ids = []
    genre_map = {}
    category_map = {}

    def __init__(self):
        parser = OptionParser()
        parser.add_option("-i", "--input", help="The path for the input files.")
        parser.add_option("-o", "--output", help="The path for the output files.")
        parser.add_option("-k", "--key", help="api key for thetvdb.com")
        parser.add_option("-p", "--pin", help="The pin code for epgdata.com")
        parser.add_option("-d", "--day", help="The day to retrieve from epgdata.com")
        parser.add_option("-v", "--debug", action="store_true", dest="debug", help="Enable debug output")
        (options, args) = parser.parse_args()

        if options.input is not None:
            self.INPUT_PATH = options.input
        if options.output is not None:
            self.OUTPUT_PATH = options.output
        if options.key is not None:
            self.APIKEY = options.key
        if options.pin is not None:
            self.PIN = options.pin
        if options.day is not None:
            self.DAY = int(options.day)
        if options.debug:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

        self.t = tvdb_api.Tvdb(apikey=self.APIKEY)

        self.t.config['language'] = 'de'

        # evaluate timezone
        localtime = time.localtime()
        if localtime.tm_isdst:
            self.timeoffset = time.strftime("+%H%M", time.gmtime(-time.altzone))
        else:            
            self.timeoffset = time.strftime("+%H%M", time.gmtime(-time.timezone))

        # Clean old files
        self.cleanup()

        # Fetch include.zip
        self.fetch_include()

        # Fetch new files
        if self.DAY != -1:
            self.fetch_data(self.DAY)
        else:
            self.DAY = 0
            while self.DAY <= 6:
                self.fetch_data(self.DAY)
                self.DAY += 1

        # Generated Merged File
        self.generate_merged()

    def cleanup(self):
        log().debug('cleanup')

        files = os.listdir(self.INPUT_PATH)
        for filename in files:
            try:
                splitted = filename.split('_')
                file_date = datetime.datetime.strptime(splitted[0], self.DATEFORMAT)
                date = (datetime.datetime.now() + datetime.timedelta(days=-1))
                if file_date < date:
                    os.remove('{}/{}'.format(self.INPUT_PATH, filename))  
                    log().info('{} deleted.'.format(filename))

            except Exception:
                log().error('{} failed to delete.'.format(filename))

    def fetch_include(self):
        # Fetch include
        params = urllib.parse.urlencode(
            {'action': 'sendInclude', 'iOEM': 'vdr', 'pin': self.PIN, 'dataType': 'xml'})
        headers = {'Content-type': 'application/x-www-form-urlencoded', 'Cache-Control': 'no-cache'}
        conn = http.client.HTTPConnection("www.epgdata.com")
        conn.request('GET', '/index.php?{}'.format(params), None, headers)
        response = conn.getresponse()
        content_type = response.getheader('content-type')
        log().info(params)
        log().info(content_type)
        log().info(response.status)

        if response.status == 200 and content_type == 'application/x-zip-compressed':
            # Content
            content = response.read()

            # Uncompress ZIP and save XML
            xml_zip = zipfile.ZipFile(BytesIO(content))
            for name in xml_zip.namelist():
                uncompressed = xml_zip.read(name)
                output_filename = '{}/{}'.format(self.INPUT_PATH, name)
                output = open(output_filename, 'wb')
                output.write(uncompressed)
                output.close()        

    def fetch_data(self, day):
        # Exists
        date = (datetime.datetime.now() + datetime.timedelta(days=day))
        files = os.listdir(self.INPUT_PATH)
        for filename in files:
            if filename.startswith(date.strftime(self.DATEFORMAT)):
                log().info('Already fetched.')
                return

        # Fetch data
        params = urllib.parse.urlencode(
            {'action': 'sendPackage', 'iOEM': 'vdr', 'dayOffset': self.DAY, 'pin': self.PIN, 'dataType': 'xml'})
        headers = {'Content-type': 'application/x-www-form-urlencoded', 'Cache-Control': 'no-cache'}
        conn = http.client.HTTPConnection("www.epgdata.com")
        conn.request('GET', '/index.php?{}'.format(params), None, headers)
        response = conn.getresponse()
        content_type = response.getheader('content-type')
        log().info(params)
        log().info(content_type)
        log().info(response.status)

        if response.status == 200 and content_type == 'application/x-zip-compressed':
            # Content
            content = response.read()

            # Uncompress ZIP and save XML
            xml_zip = zipfile.ZipFile(BytesIO(content))
            for name in xml_zip.namelist():
                uncompressed = xml_zip.read(name)
                output_filename = '{}/{}'.format(self.INPUT_PATH, name)
                output = open(output_filename, 'wb')
                output.write(uncompressed)
                output.close()

    def generate_merged(self):
        log().debug('generate_merged')
        
        # New xml
        impl = getDOMImplementation()
        new_doc = impl.createDocument(None, "tv", None)
        top_element = new_doc.documentElement
        top_element.setAttribute("generator-info-name", 'EPGData2XMLTV')
        top_element.setAttribute("generator-info-url", 'http://github.com/ortegaangelo/EPGData2XMLTV')
                
        # Categories '{}/{}'.format(self.INPUT_PATH, 'category.xml')
        self.parse_categories('{}/{}'.format(self.INPUT_PATH, 'category.xml'))

        # Genres
        self.parse_genres('{}/{}'.format(self.INPUT_PATH, 'genre.xml'))    

        # Channels
        self.generate_channel_data(top_element, '{}/{}'.format(self.INPUT_PATH, 'channel_y.xml'))

        # Program data
        files = glob.glob(self.INPUT_PATH + "/*.xml")
        for xml_file in files:
            top_element = self.generate_program_data(top_element, xml_file)

        # Write to file
        xml = ['<?xml version="1.0" encoding="utf-8" standalone="yes"?>', top_element.toprettyxml()]
        file_handle = open(self.OUTPUT_PATH + "/epg.xml", "w")
        file_handle.write(''.join(xml))
        file_handle.close()

    def parse_categories(self, epg_path):
        log().debug('parse_categories: ' +epg_path)

        context = cElementTree.iterparse(epg_path, events=("start", "end",))
        context = iter(context)
        event, root = next(context)
        for event, elem in context:
            if event == "end" and elem.tag == 'data':
                key =  elem.findtext('ca0')
                value =  elem.findtext('ca1')                
                self.category_map[key] = value
                root.clear()
            
    def parse_genres(self, epg_path):
        log().debug('parse_genres: ' +epg_path)
        
        context = cElementTree.iterparse(epg_path, events=("start", "end",))
        context = iter(context)
        event, root = next(context)
        for event, elem in context:
            if event == "end" and elem.tag == 'data':
                key =  elem.findtext('g0')
                value =  elem.findtext('g1')                
                self.genre_map[key] = value
                root.clear()

    def generate_channel_data(self, parent, channel_path):
        log().debug('generate_channel_data: ' +channel_path)

        context = cElementTree.iterparse(channel_path, events=("start", "end",))
        context = iter(context)
        event, root = next(context)
        for event, elem in context:
            if event == "end" and elem.tag == 'data':                    
                child = self.generate_channel_element(elem)
                if child is not None:
                    parent.appendChild(child)
                root.clear()

    def generate_channel_element(self, elem):

        # Read channel filter
        with open("channelfilter") as f:
            channel_filter = f.read().splitlines()

        tvchannel_id = elem.findtext('ch4')
        tvchannel_name = elem.findtext('ch0')

        if tvchannel_name not in channel_filter:
            return None

        log().debug('generate_channel_element: ' +tvchannel_name)

        impl = getDOMImplementation()
        new_doc = impl.createDocument(None, "channel", None)
        top_element = new_doc.documentElement
        top_element.setAttribute("id", tvchannel_id)

        if len(tvchannel_name)>0:
            display_name_node = new_doc.createElement("display-name")
            display_name_node.setAttribute("lang", 'de')
            display_name = new_doc.createTextNode(tvchannel_name)
            display_name_node.appendChild(display_name)
            top_element.appendChild(display_name_node)

        self.channel_ids.append(tvchannel_id)

        return top_element

    def generate_program_data(self, parent, epg_path):
        log().debug('generate_program_data: ' +epg_path)

        context = cElementTree.iterparse(epg_path, events=("start", "end",))
        context = iter(context)
        event, root = next(context)
        for event, elem in context:
            if event == "end" and elem.tag == 'data':                    
                child = self.generate_program_element(elem)
                if child is not None:
                    parent.appendChild(child)
                root.clear()

        return parent

    def generate_program_element(self, elem):
        tvchannel_id = elem.findtext('d2')
        if tvchannel_id not in self.channel_ids:
            return None

        #broadcast_id = elem.findtext('d0')
        starttime = elem.findtext('d4')
        endtime = elem.findtext('d5')
        tvshow_length = elem.findtext('d7')
        primetime = elem.findtext('d9')
        category_id = elem.findtext('d10')
        age_marker = elem.findtext('d16')
        title = elem.findtext('d19')
        subtitle = elem.findtext('d20')
        comment_long = elem.findtext('d21')
        genreid = elem.findtext('d25')
        sequence = elem.findtext('d26')
        tvd_total_value = elem.findtext('d30')
        country = elem.findtext('d32')
        year = elem.findtext('d33')
        moderator = elem.findtext('d34')
        studio_guest = elem.findtext('d35')
        regisseur = elem.findtext('d36')
        actor = elem.findtext('d37')
        image_big = elem.findtext('d40')

        log().debug('generate_program_element: ' +title)

        impl = getDOMImplementation()
        new_doc = impl.createDocument(None, "programme", None)
        top_element = new_doc.documentElement
        top_element.setAttribute("channel", tvchannel_id)
        top_element.setAttribute("start", datetime.datetime.strptime(starttime, '%Y-%m-%d %H:%M:%S')
                                 .strftime('%Y%m%d%H%M%S '+self.timeoffset))
        top_element.setAttribute("stop", datetime.datetime.strptime(endtime, '%Y-%m-%d %H:%M:%S')
                                 .strftime('%Y%m%d%H%M%S '+self.timeoffset))

        if len(title)>0:
            title_node = new_doc.createElement("title")
            title_node.setAttribute("lang", 'de')
            title_text = new_doc.createTextNode(title)
            title_node.appendChild(title_text)
            top_element.appendChild(title_node)

        if len(subtitle)>0:
            subtitle_node = new_doc.createElement("sub-title")
            subtitle_node.setAttribute("lang", 'de')
            subtitle_text = new_doc.createTextNode(subtitle)
            subtitle_node.appendChild(subtitle_text)
            top_element.appendChild(subtitle_node)

        if len(comment_long)>0:
            desc_node = new_doc.createElement("desc")
            desc_node.setAttribute("lang", 'de')
            desc_text = new_doc.createTextNode(comment_long)
            desc_node.appendChild(desc_text)
            top_element.appendChild(desc_node)

        director_list = []
        if len(regisseur)>0:
            parts = regisseur.split('|')
            for name in parts:
                director_list.append(name)
        
        actor_list = []
        if len(actor)>0:
            parts = actor.split(') - ')
            for name in parts:
                actor_list.append(name+")")

        presenter_list = []
        if len(moderator)>0:
            parts = moderator.split('|')
            for name in parts:
                presenter_list.append(name)

        guest_list = []
        if len(studio_guest)>0:
            parts = studio_guest.split('|')
            for name in parts:
                guest_list.append(name)

        if len(director_list)>0 or len(actor_list)>0 or len(presenter_list)>0 or len(guest_list)>0:
            credits_node = new_doc.createElement("credits")
            for name in director_list:
                director_node = new_doc.createElement("director")
                director_text = new_doc.createTextNode(name)
                director_node.appendChild(director_text)
                credits_node.appendChild(director_node)
            for name in actor_list:                
                actor_node = new_doc.createElement("actor")                
                actor_name = re.findall(r"(.*?)\(.*?\)", name)
                #actor_role = re.findall(r"(?<=\().*?(?=\))", name)
                if len(actor_name)>0:
                    if len(actor_name[0].strip())>0:
                        actor_text = new_doc.createTextNode(actor_name[0].strip())
                        actor_node.appendChild(actor_text)
                else:            
                    actor_text = new_doc.createTextNode(name)
                    actor_node.appendChild(actor_text)
                #if actor_role and len(actor_role[0])>0:
                #    actor_node.setAttribute("role", actor_role[0])
                if actor_node.hasChildNodes():
                    credits_node.appendChild(actor_node)
            for name in presenter_list:
                presenter_node = new_doc.createElement("presenter")
                presenter_text = new_doc.createTextNode(name)
                presenter_node.appendChild(presenter_text)
                credits_node.appendChild(presenter_node)
            for name in guest_list:                
                guest_node = new_doc.createElement("guest")
                guest_text = new_doc.createTextNode(name)
                guest_node.appendChild(guest_text)
                credits_node.appendChild(guest_node)
            top_element.appendChild(credits_node)

        if self.category_map.__contains__(category_id):
            category_node = new_doc.createElement("category")
            category_node.setAttribute("lang", 'de')
            category_text = new_doc.createTextNode(self.category_map[category_id] )
            category_node.appendChild(category_text)
            top_element.appendChild(category_node)

        if self.genre_map.__contains__(genreid):
            category_node = new_doc.createElement("category")
            category_node.setAttribute("lang", 'de')
            category_text = new_doc.createTextNode(self.genre_map[genreid] )
            category_node.appendChild(category_text)
            top_element.appendChild(category_node)

        # if len(broadcast_id)>0:
        #     category_node = new_doc.createElement("category")
        #     category_node.setAttribute("lang", 'de')
        #     category_text = new_doc.createTextNode(broadcast_id)
        #     category_node.appendChild(category_text)
        #     top_element.appendChild(category_node) 

        if len(tvshow_length)>0:            
            length_node = new_doc.createElement("length")
            length_node.setAttribute("units", 'minutes')
            length_text = new_doc.createTextNode(tvshow_length)
            length_node.appendChild(length_text)
            top_element.appendChild(length_node)

        #if int(sequence) is not 0:
        #    episode_node = new_doc.createElement("episode-num")
        #    episode_node.setAttribute("system","onscreen")
        #    episode_text = new_doc.createTextNode(sequence)
        #    episode_node.appendChild(episode_text)
        #    top_element.appendChild(episode_node)

        if int(sequence) != 0:
            episode_node = new_doc.createElement("episode-num")
            episode_node.setAttribute("system","xmltv_ns")
            season = ""
            episodeNumber = str(int(sequence)-1)
            if len(subtitle)>0:
                try:
                    seriesName = title.split(' - ')
                    episodeName = subtitle.split(' / ')
                    episode_tvdb = self.t[seriesName[0]].search(episodeName[0], key='episodeName')
                    if len(episode_tvdb)>0:
                        season = str(episode_tvdb[0]['airedSeason']-1)
                        episodeNumber = str(episode_tvdb[0]['airedEpisodeNumber']-1)
                        log().debug('episode: ' +title + '.' + subtitle + '.' + season + '.' + episodeNumber)
                except tvdb_api.tvdb_shownotfound:
                    log().info("Could not find episode for: " + title + '.' + subtitle)
                except tvdb_api.tvdb_error:
                    log().error("tvdb error: " + title + '.' + subtitle)
                except KeyError:
                    log().error("Mapping key not found: " + title + '.' + subtitle)
                except:
                    log().error("Unexpected error:")
                    log().error(traceback.format_exc())

            episode_text = new_doc.createTextNode(season+"."+episodeNumber+".")
            episode_node.appendChild(episode_text)
            top_element.appendChild(episode_node)
            
        if len(country)>0:
            parts = country.split('|')
            for c in parts:
                country_node = new_doc.createElement("country")
                country_text = new_doc.createTextNode(c)
                country_node.appendChild(country_text)
                top_element.appendChild(country_node)

        if len(year)>0:
            year_node = new_doc.createElement("date")
            year_text = new_doc.createTextNode(year)
            year_node.appendChild(year_text)
            top_element.appendChild(year_node)

        if len(image_big)>0:
            src_node = new_doc.createElement("icon")
            src_node.setAttribute("src", image_big)
            top_element.appendChild(src_node)

        if len(age_marker)>0:
            rating_node = new_doc.createElement("rating")
            value_node = new_doc.createElement("value")
            rating_text = new_doc.createTextNode(age_marker)
            value_node.appendChild(rating_text)
            rating_node.appendChild(value_node)
            top_element.appendChild(rating_node)

        if int(tvd_total_value)>0:
            star_rating_node = new_doc.createElement("star-rating")
            value_node = new_doc.createElement("value")
            rating = int(tvd_total_value)-1
            star_rating_text = new_doc.createTextNode(str(rating) + "/4")
            value_node.appendChild(star_rating_text)
            star_rating_node.appendChild(value_node)
            top_element.appendChild(star_rating_node)

        if int(primetime)==1:
            premiere_node = new_doc.createElement("premiere")
            premiere_text = new_doc.createTextNode("Premiere")
            premiere_node.appendChild(premiere_text)
            top_element.appendChild(premiere_node)

        return top_element

App()
