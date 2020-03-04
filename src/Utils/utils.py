from textblob import TextBlob as tb
from itertools import chain
from collections import defaultdict
import traceback
from pprint import pprint
import re
import os
import sys
import ujson
import csv
from nltk import sent_tokenize as tkn
from functools import reduce
import math
import shutil

# class Utils(object):
# 	def __init__():
# 		pass

# global 

sample_text = "India's national cricket team is one of the most televised sports teams in the world. So what better way to spread the word about your brand and company than to adorn the signature blue jerseys of the team. BYJU'S reportedly has that much-coveted spot now, replacing electronics brand OPPO.  OPPO had paid Rs 1,079 Cr for the right to be on the jersey in 2017 but now finds the price too high and unsustainable. It's allocating the rights to BYJU'S according to a Times Of India Report, and work on the front is already in progress. OPPO would also pay BYJU'S, a small fee to cover for the expenses of being the sponsor, while the Board for Control of Cricket in India (BCCI) would continue to receive the same amount that OPPO had bid. BYJU'S would hold the rights for the sponsorship till March 2022.  After back-to-back massive funding rounds over the last year, the Bengaluru-based edtech unicorn tripled its revenue to INR 1,430 Cr ($204.36 Mn) in FY18-19. This helped the company turn profitable on a full-year basis.  Interestingly, BYJU'S sponsorship deal comes at a time when the company is looking to expand overseas. Being the sponsor of the Indian cricket team which draws throngs of fans from around the world is the perfect exposure vehicle for BYJU'S as it looks to take on global edtech players.  The company had raised $150 Mn from Qatar's sovereign wealth fund, the Qatar Investment Authority (QIA) earlier this month. BYJU'S said it would use this funding for international expansion and tailoring its product for a global audience. Byju Raveendran, cofounder and CEO, BYJU'S, said, \u201cThis partnership will support and strengthen our vision of creating and delivering personalised learning experiences to students. This will help us explore and leverage our expertise in creating immersive tech-enabled learning programs for students in smaller cities, regions and newer markets.\u201d  Founded in 2008 by Divya Gokulnath and Raveendran, BYJU'S offers a learning app, which was launched in 2015 and has learning programmes for students in classes IV-XII along with courses to help students prepare for competitive exams like JEE, NEET, CAT, IAS, GRE, and GMAT.  BYJU'S was last valued at $ 5Bn and has raised over $819.8 Mn funding from investors such as General Atlantic, Tencent, Naspers, and Canada's Pension Plan Investment Board (CPPIB) among others."

def get_noun_words_and_ngrams(text=None):
	if text is None:
		return []
	all_noun_ngrams = []
	try:
		text = re.sub("\""," ",text)
		text = re.sub("\'"," ",text)
		text = re.sub("‘"," ",text)
		text = re.sub("“"," ",text)
		text = re.sub("’"," ",text)
		text = re.sub("”"," ",text)
		text = re.sub("\s+"," ",text)
		text = text.replace("'s ","s")
		text = text.replace("n't "," not ")
		text = text.replace("' "," ")

		text_lower = text.lower()

		wiki = tb(text)

		consecutive_noun_words = []
		noun_words = []
		for word,tag in wiki.tags:
			if tag in ["NNP","NNPS","NN","NNS"]:
				noun_words.append(word.strip('()./\\>,<?:;!@#$%^&*\{\}+'))
			else:
				if len(noun_words) > 0:
					consecutive_noun_words.append(noun_words)
					noun_words = []
		
		for each_list in consecutive_noun_words:
			ngrams = list("_".join(w.lower() for w in each_list[0:i+1]) for i in range(len(each_list)))
			ngrams.extend(each_list)
			if len(ngrams) > 0:
				ngrams = [ng for ng in ngrams if ng != ""]
			all_noun_ngrams.extend(ngrams)
	except:
		pass
	return all_noun_ngrams

def get_word_lists(line):
	line = re.sub("\""," ",line)
	line = re.sub("\'"," ",line)
	line = re.sub("‘"," ",line)
	line = re.sub("“"," ",line)
	line = re.sub("’"," ",line)
	line = re.sub("”"," ",line)
	line = re.sub("\s+"," ",line)
	line = line.replace("'s "," ")
	line = line.replace("n't "," not ")
	line = line.replace("' "," ")

	line_lower = line.lower()

	wiki = tb(line)
	# print("WikiType:")
	# print(type(wiki))
	noun_phrases = wiki.noun_phrases
	tags = wiki.tags

	word_properties = defaultdict(lambda : defaultdict(dict))

	# Process line (i.e. story text)
	proper_noun_words_list = []
	other_noun_words_list = []
	stop_words_set = set()
	# Some lambdas for use later in code
	func_word_count_nnp_share = lambda x:word_properties["words"].get(x,{}).get("as_proper",0)
	func_word_count_nns_share = lambda x:word_properties["words"].get(x,{}).get("as_other",0)
	func_word_count_stop_share = lambda x:word_properties["words"].get(x,{}).get("as_stop",0)
	func_word_share = lambda x : (func_word_count_nnp_share(x),
						func_word_count_nns_share(x),
						func_word_count_stop_share(x))
	word_shares = lambda word:func_word_share(word) # returns tuple (times_word_occurred_as_nnp, times_word_occurred_as_nns, times_word_occurred_as_other_noun)
	word_as_which_noun = lambda w_shares:"nnp" if max(w_shares) == w_shares[0] else ("nns" if max(w_shares) == w_shares[1] else "stop")

	for word,tag in tags:
		if len(word) > 2:
			if word.lower() not in word_properties["words"]:
				word_properties["words"][word.lower()] = {}
			if tag in ["NNP","NNPS"]:
				proper_noun_words_list.append(word.lower())
				if "as_proper" in word_properties["words"][word.lower()]:
					word_properties["words"][word.lower()]["as_proper"] += 1
				else:
					word_properties["words"][word.lower()]["as_proper"] = 1
			elif tag in ["NN","NNS"]:
				other_noun_words_list.append(word.lower())
				if "as_other" in word_properties["words"][word.lower()]:
					word_properties["words"][word.lower()]["as_other"] += 1
				else:
					word_properties["words"][word.lower()]["as_other"] = 1
			else:
				if "as_stop" in word_properties["words"][word.lower()]:
					word_properties["words"][word.lower()]["as_stop"] += 1
				else:
					word_properties["words"][word.lower()]["as_stop"] = 1
				stop_words_set.add(word.lower())

	for word,tag in tags:
		if len(word) > 2:
			w_low = word.lower()
			# print(w_low)
			as_nountype_counts = word_shares(w_low)
			total_count = reduce((lambda x,y:x+y),as_nountype_counts,0)
			word_properties["words"][w_low]["sh_nnp"] = as_nountype_counts[0]/total_count
			word_properties["words"][w_low]["sh_nns"] = as_nountype_counts[1]/total_count
			word_properties["words"][w_low]["sh_stop"] = as_nountype_counts[2]/total_count
	
	# Operation A
	# A1. make a set from list of Proper Noun words
	proper_noun_words_set = set(proper_noun_words_list)
	
	noun_word_counts = defaultdict(int)
	for word in proper_noun_words_list:
		noun_word_counts[word] += 1

	noun_phrases = [ph for ph in noun_phrases if ph not in proper_noun_words_set]

	# A2. Filter out those words from the Proper Noun words set
	# which are seen together consecutively in a phrase
	dummy_phrase_replacements = {}	
	for phrase in set(noun_phrases):
		if phrase not in word_properties["phrases"]:
			word_properties["phrases"][phrase] = {}
		phrase_words = phrase.split()
		is_proper_noun_phrase = True
		for word in phrase_words:
			if word not in proper_noun_words_list:
				is_proper_noun_phrase = False
				break
		if is_proper_noun_phrase:
			if "as_proper" in word_properties["phrases"][phrase]:
				word_properties["phrases"][phrase]["as_proper"] += noun_phrases.count(phrase)
			else:
				word_properties["phrases"][phrase]["as_proper"] = noun_phrases.count(phrase)
			word_properties["phrases"][phrase]["sh_nnp"] = 1
			word_properties["phrases"][phrase]["sh_stop"] = 0
			word_properties["phrases"][phrase]["sh_nns"] = 0
			
			# Step1 This phrase's individual words should not be counted (we are making this rule!), so remove the word from proper noun words list
			# Step2 But for each extra occurrence of the current phrase's individual words in the nouns list, increase their count in the phrase list
			count_of_words_in_phrase = []
			removed = False
			for word in phrase_words:
				# Step1
				try:
					proper_noun_words_set.remove(word)
					removed = True
					# print(f"Removed [{word}] (occurrence:{str(proper_noun_words_list.count(word))}) from proper noun set as it's a part of - [{phrase}]")
				except:
					# print(f"Cannot remove - [{word}] from set")
					pass
				if removed:
					count_of_words_in_phrase.append((word,proper_noun_words_list.count(word)))
				
			# Step2
			try:
				if removed:
					ntimes = max(count_of_words_in_phrase, key = lambda x:x[1])[1]
					dummy_times = 0
					if isinstance(ntimes,int):
						dummy_times = ntimes - noun_phrases.count(phrase)
						# dummy_times = ntimes - (word_properties["phrases"][phrase].get("as_proper",0))
					word_properties["phrases"][phrase]["as_proper"] += dummy_times
					# print(f"Added [{phrase}] {str(dummy_times)} times = [{str(ntimes)}(total expected replacements)] - [{str(noun_phrases.count(phrase))}(already in phrases)] to proper_noun_words_list")
						# f"Added [{phrase}] {str(dummy_times)} times = [{str(ntimes)}(total expected replacements)] - [{str((word_properties['phrases'][phrase].get('as_proper',0)))}(already in phrases)] to proper_noun_words_list")
			except:
				print(traceback.format_exc())
				pass

		else:
			if "as_other" in word_properties["phrases"][phrase]:
				word_properties["phrases"][phrase]["as_other"] += 1
			else:
				word_properties["phrases"][phrase]["as_other"] = 1
			# sh_prp = share_of_proper_noun_in_phrase
			# word_count = max((word_properties["words"].get(word,{}).get("as_proper",0) + word_properties["words"].get(word,{}).get("as_other",0) + word_properties["words"].get(word,{}).get("as_stop",0)),1)
			# word_as_which_noun = lambda :"nnp" if max(word_shares(x)) == word_shares(x)[0] else ("nns" if max(word_shares(x)) == word_shares(x)[1] else "stop")
			count_nnps = lambda ph_wrds: reduce((lambda x,y:x+y),[1 for word in ph_wrds if word_as_which_noun(word_shares(word)) == "nnp"],0)
			count_nns = lambda ph_wrds: reduce((lambda x,y:x+y),[1 for word in ph_wrds if word_as_which_noun(word_shares(word)) == "nns"],0)
			count_other = lambda ph_wrds: reduce((lambda x,y:x+y),[1 for word in ph_wrds if word_as_which_noun(word_shares(word)) == "stop"],0)
			# print("Phrase: " + phrase)
			# print("count_nnps " + str(count_nnps(phrase_words)))
			# print("count_nns " + str(count_nns(phrase_words)))
			# print("count_other " + str(count_other(phrase_words)))
			try:
				word_properties["phrases"][phrase]["sh_nnp"] = count_nnps(phrase_words)/len(phrase_words)
			except ZeroDivisionError:
				word_properties["phrases"][phrase]["sh_nnp"] = 0
			except:
				print(traceback.format_exc())
				print("Cannot count word's share as a nnp word - utils.py")
				pass
			try:
				word_properties["phrases"][phrase]["sh_nns"] = count_nns(phrase_words)/len(phrase_words)
				# word_properties["phrases"][phrase]["sh_stop"] = len(
					# [word for word in phrase_words if (
					# 	word_properties["words"].get(word,{}).get("as_stop",0) == max(
					# 		word_properties["words"].get(word,{}).get("as_proper",0), 
					# 		word_properties["words"].get(word,{}).get("as_other",0), 
					# 		word_properties["words"].get(word,{}).get("as_stop",0) / max(,1)
					# 		)
					# 	)])/len(phrase_words)
			except ZeroDivisionError:
				word_properties["phrases"][phrase]["sh_nns"] = 0
			except:
				print(traceback.format_exc())
				print("Cannot count word's share as a nns word - utils.py")
				pass
			try:
				word_properties["phrases"][phrase]["sh_stop"] = count_other(phrase_words)/len(phrase_words)
				# word_properties["phrases"][phrase]["sh_stop"] = len(
					# [word for word in phrase_words if (
					# 	word_properties["words"].get(word,{}).get("as_stop",0) == max(
					# 		word_properties["words"].get(word,{}).get("as_proper",0), 
					# 		word_properties["words"].get(word,{}).get("as_other",0), 
					# 		word_properties["words"].get(word,{}).get("as_stop",0) / max(,1)
					# 		)
					# 	)])/len(phrase_words)
			except ZeroDivisionError:
				word_properties["phrases"][phrase]["sh_stop"] = 0
			except:
				print(traceback.format_exc())
				print("Cannot count word's share as a stop word - utils.py")
				pass

	# Generate share of the phrase as 

	# A3. If there are any proper nouns left still, keep those
	imp_proper_noun_words_list = [word for word in proper_noun_words_list if word in proper_noun_words_set]

	
	return {"phrases":noun_phrases,"np_words":imp_proper_noun_words_list,"nn_words":other_noun_words_list,"word_properties":word_properties,"dummy_replacements":dummy_phrase_replacements,"stop_words":stop_words_set}

def get_imp_words(line,title=""):
	try:
		phrases,nnps,nns,prps,replacements,stop_set = get_word_lists(line).values()
		ttl_phrases,ttl_nnps,ttl_nns,ttl_prps,_,_ = get_word_lists(title).values()

		for phrase in replacements:
			wrd_cnts = []
			for r_word in replacements[phrase]:
				if r_word in ttl_nnps:
					wrd_cnts.append(ttl_nnps.count(r_word))
					ttl_nnps.remove(r_word)
			dummy_times = max(wrd_cnts) - ttl_phrases.count(phrase)
			if dummy_times and dummy_times > 0:
				try:
					if phrase not in ttl_prps["phrases"]:
						ttl_prps["phrases"][phrase] = {}
						ttl_prps["phrases"][phrase]["as_proper"] = 0
					ttl_prps["phrases"][phrase]["as_proper"] += dummy_times
				except:
					pass


		# ------ IMPORTANT COMMON NOUNS MAY BE EXTRACTED FOR CLUSTERING LATER --------
		# # care for only those common nouns that are in title
		# imp_nns_set = set(nns) & set(ttl_nns)

		# # extend the lists from story with lists from title (list is needed to keep counts)

		# nns.extend(ttl_nns)
		# # print(f"nns after:{len(nns)}")
		# # pprint(nns)
		# nns_counts = defaultdict(int)
		# for n in nns:
		# 	nns_counts[n] += 1

		# imp_nns = [n for n in nns if n in imp_nns_set and nns_counts.get(n,0) >= 2]
		# ------ IMPORTANT COMMON NOUNS MAY BE EXTRACTED FOR CLUSTERING LATER --------


		# print(f"phrases Before:{len(phrases)}")
		# pprint(phrases)
		phrases.extend(ttl_phrases)
		phrases = [ph for ph in phrases if len(ph) > 2 and (not beg_has_special_chars(ph)) and len(ph.split()) <= 4]
		# print(f"phrases after:{len(phrases)}")


		# print(f"nnps Before:{len(nnps)}")
		# pprint(nnps)
		nnps.extend(ttl_nnps)
		nnps = [word for word in nnps if word not in stop_set]

		# print(f"nnps after:{len(nnps)}")

		# Final list
		# a. all noun phrases in story text
		# b. all proper noun words from story text (excluding phrases)
		# c. all `other noun words` that are in title also
		# Collect term freqs
		story_phrase_freqs = defaultdict(int)
		story_word_freqs = defaultdict(int)
		title_word_freqs = defaultdict(int)
		title_phrase_freqs = defaultdict(int)
		for w_or_p,stats in prps["phrases"].items():
			story_phrase_freqs[w_or_p] = reduce(
				(lambda x,y:x+y),
				[v for k,v in stats.items() if k[:3] == "as_"]
				,0) 

		for w_or_p,stats in prps["words"].items():
			story_word_freqs[w_or_p] = reduce(
				(lambda x,y:x+y),
				[v for k,v in stats.items() if k[:3] == "as_"]
				,0)
		
		for w_or_p,stats in ttl_prps["words"].items():
			title_word_freqs[w_or_p] = reduce(
				(lambda x,y:x+y),
				[v for k,v in stats.items() if k[:3] == "as_"]
				,0)

		for w_or_p,stats in ttl_prps["phrases"].items():
			title_phrase_freqs[w_or_p] = reduce(
				(lambda x,y:x+y),
					[v for k,v in stats.items() if k[:3] == "as_"]
					,0)
		# pprint("title_phrase_freqs")
		# pprint(title_phrase_freqs)
		# pprint("title_word_freqs")
		# pprint(title_word_freqs)
		# pprint("story_phrase_freqs")
		# pprint(story_phrase_freqs)
		# pprint("story_word_freqs")
		# pprint(story_word_freqs)

		f_list = {}
		# all_words_or_phrases_set = set(phrases) | (x,"ph") for x in set(nnps)
		# Final phrases
		for f_ph in set(phrases):
			# check in phrases stats
			# f_list[f_ph] = {}
			if f_ph in prps["phrases"]:
				f_list[f_ph] = prps["phrases"][f_ph]
				f_list[f_ph]["in_story_freq"] = story_phrase_freqs.get(f_ph,0)
			if f_ph in ttl_prps["phrases"]:
				if f_ph in f_list:
					f_list[f_ph]["in_title_freq"] = title_phrase_freqs.get(f_ph,0)
			if f_ph in f_list:
				f_list[f_ph]["total_freq"] = story_phrase_freqs.get(f_ph,0) + title_phrase_freqs.get(f_ph,0)
		# Final Words
		for f_w in set(nnps):
			# check in phrases stats
			f_list[f_w] = {}
			if f_w in prps["words"]:
				f_list[f_w] = prps["words"][f_w]
				f_list[f_w]["in_story_freq"] = story_word_freqs.get(f_w,0)
			if f_w in ttl_prps["words"]:
				if f_w in f_list:
					f_list[f_w]["in_title_freq"] = title_word_freqs.get(f_w,0)
			if f_w in f_list:
				f_list[f_w]["total_freq"] = story_word_freqs.get(f_w,0) + title_word_freqs.get(f_w,0)

		
		pop = []
		set_phrases = set(phrases)
		for ph,ph_stats in f_list.items():
			ph_split = ph.split()
			if len(ph_split) > 1:
				if ph_stats.get("as_other",0) > 0:
					for bigram_tup in zip(ph_split,ph_split[1:]):
						bigram = " ".join(bigram_tup)
						if bigram in set_phrases and bigram in f_list and f_list[bigram].get("as_proper",0) > 0:
							try:
								# Transfer the frequency of the phrase to that of the noun_phrase
								if "as_proper" in f_list[bigram]:
									f_list[bigram]["as_proper"] += f_list[ph].get("in_story_freq",0) + f_list[ph].get("in_title_freq",0)
								else:
									f_list[bigram]["as_proper"] = f_list[ph].get("in_story_freq",0) + f_list[ph].get("in_title_freq",0)
								if "in_story_freq" in f_list[bigram]:
									f_list[bigram]["in_story_freq"] += f_list[ph].get("in_story_freq",0) + f_list[ph].get("in_title_freq",0)
									f_list[bigram]["total_freq"] += f_list[ph].get("in_story_freq",0) + f_list[ph].get("in_title_freq",0)
								else:
									f_list[bigram]["in_story_freq"] = f_list.get("in_story_freq",0) + f_list.get("in_title_freq",0)
									f_list[bigram]["total_freq"] = f_list[ph].get("in_story_freq",0) + f_list[ph].get("in_title_freq",0)
								pop.append(ph)
							except:
								print(traceback.format_exc())
								print(f"Cannot add {ph}'s {bigram} presence in noun_phrases")
			if ph_stats.get("sh_stop",0) > 0 or ph_stats.get("sh_nns",0) > 0:
				if ph_stats.get("total_freq",0) < 2:
					pop.append(ph)
					continue
			if ph_stats.get("as_proper",0) > 0 and ph_stats.get("total_freq",0) == 1:
				pop.append(ph)
				continue
			if ph_stats.get("as_other",0) > 0 and ph_stats.get("total_freq",0) < 3:
				pop.append(ph)
				continue
			if len(ph) <= 2:
				pop.append(ph)
				continue

		for p in pop:
			try:
				f_list.pop(p)
			except:
				pass

		phrase_to_orig_case_mapping = {}
		for phrase in f_list:
			# print(f"Checking case for {phrase}")
			try:
				phrase_original_form = max([(line[x:y],len(line[x:y])) for x,y in [match.span() for match in re.finditer(f"{phrase}",line.lower())]],key= lambda x:x[1])
				# if phrase_original_form and (len(phrase_original_form) >= len(phrase)):
				if phrase_original_form and (len(phrase_original_form[0]) == len(phrase)):
					f_list[phrase]["origCase"] = phrase_original_form[0]
				# else:
				# 	f_list[phrase]["origCase"] = "Undeterminable"
				# phrase_forms = [line[x:y] for x,y in [match.span() for match in re.finditer(f"{phrase}",line.lower())]]
				# phrase_forms_set = set(phrase_forms)
				# # print(phrase_forms_set)
				# if phrase_forms_set:
				# 	if len(phrase_forms_set) > 1:
				# 		f_list[phrase]["origCase"] = min([(form,form[1]) for form in phrase_forms_set],key=lambda x:x[1]) [1]
				# 	elif (len(phrase_forms_set) == 1) and (len(phrase_forms[0]) == len(phrase)):
				# 		f_list[phrase]["origCase"] = phrase_forms[0]
			except:
				# print(f"forms cant be determined for: {phrase}")
				# f_list[phrase]["origCase"] = "Undeterminable"
				# print(traceback.format_exc())
				pass	

		# pprint(f_list)
		# exit()
		# print("lambdas work, yo")
		return f_list
	except:
		print(traceback.format_exc())
		return None

def strip_special_chars(s):
	special_chars = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~‘’“”"
	return s.strip(special_chars)

def beg_has_special_chars(s):
	special_chars = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~‘’“”"
	return s[0] in special_chars

def get_longest_form(word,forms=[]):
	pass

def filter_phrases(el):
	try:
		phrase = el[0]
		stats = el[1]
		if stats.get("freq",0) < 2:
			return False
		phrase_words = phrase.split()
		length = len(phrase_words)
		if length == 1:
			if not len(strip_special_chars(phrase_words[0])) >= 3:
				return False
		elif length == 2:
			for word in phrase_words:
				if not len(strip_special_chars(word)) >= 3:
					return False
		elif length > 4:
			return False
		return True
	except:
		return True

def filter_nnps(el):
	if el[1].get("freq") < 2:
		return False
	return True

def create_dir_if_not_exist(abs_dir_path=None):
	if abs_dir_path is None:
		err_msg = f"Path to the desired directory is None, cannot create."
		raise Exception(err_msg)
	else:
		try:
			if not (os.path.exists(abs_dir_path) and os.path.isdir(abs_dir_path)):
				os.makedirs(abs_dir_path)
				return abs_dir_path
			else:
				return abs_dir_path
		except:
			raise

def create_directories_if_not_exist(dir_name = None):
            
        def make_dirs(dcopy):
            try:
                if not (os.path.exists(dcopy) and os.path.isdir(dcopy)):
                    os.makedirs(dcopy)
            except:
                print(f"Can not create directory. Invalid path: {dcopy}")
                raise

        if dir_name is None:
            print(f"Cannot create directory with path: None")
            raise
        if isinstance(dir_name, str):
            make_dirs(dir_name)
        elif isinstance(dir_name, list):
            for d in dir_name:
                make_dirs(d)
        else:
            pass

def remove_output_file_if_exists(output_file=None):
	# Remove the results file, if it exists already
	if os.path.exists(output_file):
		if os.path.isfile(output_file):
			print("Output File already exists, deleting")
			os.remove(output_file)
			print("Done")
		elif os.path.isdir(output_file):
			print("Output Directory already exists, deleting")
			shutil.rmtree(output_file)
			print("Done")
	else:
		# print("Output Path DOES NOT already exist.") # DEBUG
		pass


if __name__ == "__main__":
	print(get_noun_words_and_ngrams(sample_text))