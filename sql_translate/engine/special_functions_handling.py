from typing import Dict, List
import json
import os
from sql_translate.engine import regex
from sqlparse.tokens import Literal
import logging

# case insensitive wrapper enforcing that re methods actually have an impact
Regex = regex.Regex()


class _SpecialFunctionHandler():
    def __init__(self) -> None:
        pass


class SpecialFunctionHandlerImpalaToPresto(_SpecialFunctionHandler):
    def __init__(self) -> None:
        self.special_functions = {
            "array": self.array,
            "count": self.count,
            "extract": self.extract,
            "from_utc_timestamp": self.from_utc_timestamp,
            "over": self.over,
            "isnull": self.isnull,
            "unix_timestamp": self.unix_timestamp,
            "round" : self.round,
            "locate" : self.locate,
            "ifnull" : self.ifnull,
            "dround" : self.round,
            "rand" : self.rand,
            "randome" : self.randome,
            "trunc" : self.trunc,
            "dtrunc" : self.dtrunc,
            "truncate" : self.truncate,
            "from_timestamp" : self.from_timestamp,
            "to_timestamp" : self.to_timestamp,
            "nvl" : self.nvl,
            "nvl2" : self.nvl2,
            "from_unixtime" : self.from_unixtime,
            "months_between" : self.months_between,
            "date_part" : self.date_part,
            "dayofweek" : self.dayofweek,
            "dayname" : self.dayname
        }

        # Load the date time parsing dictionary
        with open(os.path.join(os.path.dirname(__file__), "..", "datetime_parsing_dictionaries", "impala_to_presto", "datetime_parsing.json")) as f:
            self.datetime_translation = json.load(f)

        with open(os.path.join(os.path.dirname(__file__), "..", "datetime_parsing_dictionaries", "impala_to_presto", "timezones.json")) as f:
            self.timezone_translation = json.load(f)

    def over(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable to the OVER clause (handled like a function by this python package)
        Example: CAST( MIN(pkey) OVER(PARTITION BY primkey, CONCAT(CAST(a AS VARCHAR(20)), CAST(b AS VARCHAR(20))) order by c desc) as DATE ) -->
        cast(min(pkey) over(PARTITION BY primkey, concat(cast(cast(a AS varchar(20)) AS varchar), cast(cast(b AS varchar(20)) AS varchar)) ORDER BY c DESC) AS date)

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        if function_info['translation']== function_name_formatting:
            function_name_formatting=''
        arguments = []
        skip = 0
        for idx, argument in enumerate(str_output_arguments_composed):
            # Skip next entry when merging a keyword made of multiple words
            if skip:
                skip -= 1
                continue

            # Merge keywords that are actually made of of multiple words
            if Regex.search(r"\bnulls\s+(first|last)\b", argument, strict=False):  # ORDER BY col NULLS LAST
                if 0 < idx:
                    result = Regex.search(r"\bnulls\s+(?P<order>first|last)\b", argument)
                    arguments[-1] += f" NULLS {result['order'].upper()}"
            elif argument.upper() == "PARTITION":  # PARTITION BY col
                if idx < len(str_output_arguments_composed) - 1:
                    if str_output_arguments_composed[idx+1].upper() == "BY":
                        arguments.append(f"{argument.upper()} {str_output_arguments_composed[idx+1].upper()}")
                        skip = 1
            elif argument.upper() in ("DESC", "ASC"):  # ORDER BY col DESC
                if 0 < idx:
                    arguments[-1] += f" {argument.upper()}"
            else:
                arguments.append(argument)

        partition_by_index, order_by_index = None, None
        for idx, argument in enumerate(arguments):  # Find PARTITION BY & ORDER BY (without useing try/cast)
            if argument.upper() == "PARTITION BY":
                partition_by_index = idx
            elif argument.upper() == "ORDER BY":
                order_by_index = idx
        translated_function_call = function_info["translation"] + function_name_formatting + "("
        if partition_by_index is not None:
            translated_function_call += "PARTITION BY "
            if order_by_index is not None:
                translated_function_call += " ".join(arguments[partition_by_index+1:order_by_index])
                translated_function_call += " ORDER BY "
                translated_function_call += " ".join(arguments[order_by_index+1:])
            else:
                translated_function_call += " ".join(arguments[partition_by_index+1:])
        else:
            if order_by_index is not None:
                translated_function_call += "ORDER BY "
                translated_function_call += " ".join(arguments[order_by_index+1:])
            else:
                translated_function_call += " ".join(arguments)
        translated_function_call += ")"
        return translated_function_call

    def count(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "count" function
        Example: count(distinct a, b) --> count(distinct cast(a AS varchar)|| ' ' ||cast(b AS varchar))

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo  (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        if Regex.search(r"^distinct\s", str_output_arguments_composed[0], strict=False):  # There could be multiple arguments if distinct is involved. String manipulations are required for proper translation.
            str_output_arguments_composed[0] = Regex.sub(r"^distinct\s", "", str_output_arguments_composed[0])
            translated_function_call = "|| ' ' ||".join([f"cast({arg} AS varchar)" for arg in str_output_arguments_composed])
            return f"count{function_name_formatting}(distinct {translated_function_call})"
        else:
            assert len(input_arguments_clean) == 1  # Should be 1 unless there is a "distinct" keyword
            return "count" + function_name_formatting + "(" + " ".join(str_output_arguments_composed) + ")"

    # Complete different syntax: array[] instead of array()

    def array(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "array" function
        Example: array(1, 2, 3) --> array[1, 2, 3]

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        return function_info["translation"] + function_name_formatting + "[" + ", ".join(str_output_arguments_composed) + "]"

        
    def from_timestamp(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "from_timestamp" function
        Example: from_timestamp(timestamp('2020-03-25 16:32:01'), 'yyyy-MM-dd') -->
        date_format(cast('2020-03-25 16:32:01' AS timestamp), '%Y-%m-%d')

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        if str_output_arguments_composed[1] == "'u'":  # Requested the day of the week. Need to convert to day_of_week() & change indexing
            return (
                "case "
                f"when day_of_week({str_output_arguments_composed[0]}) = 7 "
                "then 1 "  # Sundays now return 1
                "else "
                f"day_of_week({str_output_arguments_composed[0]}) + 1 "  # The rest is shifted
                "end"
            )
        else:
            str_output_arguments_composed[1] = self._translate_datetime_format(str_output_arguments_composed[1])
            return function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"  

    def to_timestamp(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if str_output_arguments_composed[1] == "'u'":  # Requested the day of the week. Need to convert to day_of_week() & change indexing
            return (
                "case "
                f"when day_of_week({str_output_arguments_composed[0]}) = 7 "
                "then 1 "  # Sundays now return 1
                "else "
                f"day_of_week({str_output_arguments_composed[0]}) + 1 "  # The rest is shifted
                "end"
            )
        else:
            str_output_arguments_composed[1] = self._translate_datetime_format(str_output_arguments_composed[1])
            return function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")" 
        
    def from_unixtime(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if len(input_arguments_clean)==1:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" "from_unixtime" +"(" + input_arguments_clean[0]['value'] + ")"+ ", " + "'%Y-%m-%d %H:%i:%S'" + ")"
            return f"{translated_function_call}"
        elif len(input_arguments_clean)==2:
            str_output_arguments_composed[1] = self._translate_datetime_format(input_arguments_clean[1]['value'])
            translated_function_call = function_info["translation"] + function_name_formatting + "(" +"from_unixtime" +"(" + input_arguments_clean[0]['value'] + ")" +", " + str_output_arguments_composed[1] + ")"
            return f"{translated_function_call}"

    def dayofweek(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        return (
                "case "
                f"when day_of_week({str_output_arguments_composed[0]}) = 7 "
                "then 1 "  # Sundays now return 1
                "else "
                f"day_of_week({str_output_arguments_composed[0]}) + 1 "  # The rest is shifted
                "end"
            )
    
    def dayname(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        translated_function_call = function_info["translation"] + function_name_formatting + "(" + str_output_arguments_composed[0]  +", " + "'EEEE'" + ")"
        return f"{translated_function_call}"

    # Two options depending on the second argument (integer or string)

    def unix_timestamp(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "unix_timestamp" function
        Example: unix_timestamp('2020-03-25 16:32:01') --> cast(to_unixtime(cast('2020-03-25 16:32:01' AS timestamp)) AS bigint)

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        if len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"date_parse({input_arguments_clean[0]['value']}, {self._translate_datetime_format(input_arguments_clean[1]['value'])})"]
        translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
        return f"cast({translated_function_call} AS bigint)"

    # Coverage is incomplete: timestamp -> timestamp is the only thing supported. KeyError -> need to add tz

    def from_utc_timestamp(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "from_utc_timestamp" function
        Example: from_utc_timestamp(a, 'PST') --> cast(cast(cast(a AS timestamp) as timestamp) AT TIME ZONE 'America/Los_Angeles' AS timestamp)

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        return f"cast(cast({str_output_arguments_composed[0]} as timestamp) AT TIME ZONE {self.timezone_translation[str_output_arguments_composed[1]]} AS timestamp)"

    def isnull(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "isnull" function
        Example: isnull(1) --> 1 is null

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        # assert len(input_arguments_clean) == 2
        if len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[1]['value']}, {input_arguments_clean[0]['value']}"]
        translated_function_call = " if" + function_name_formatting + "(" +  input_arguments_clean[0]['value'] +" "+ function_info["translation"]+" , " + ", ".join(str_output_arguments_composed) + ")"
        return f"{translated_function_call}"
    
    def ifnull(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "isnull" function
        Example: isnull(1) --> 1 is null

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        # assert len(input_arguments_clean) == 2
        if len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[1]['value']}, {input_arguments_clean[0]['value']}"]
        translated_function_call = " if" + function_name_formatting + "(" +  input_arguments_clean[0]['value'] +" "+ function_info["translation"]+" , " + ", ".join(str_output_arguments_composed) + ")"
        return f"{translated_function_call}"
    
    def nvl(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
    
        if len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[1]['value']}, {input_arguments_clean[0]['value']}"]
    
        translated_function_call = " if" + function_name_formatting + "(" +  input_arguments_clean[0]['value'] +" "+ function_info["translation"]+" , " + ", ".join(str_output_arguments_composed) + ")"
        return f"{translated_function_call}"
    
    def nvl2(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
    
        if len(input_arguments_clean) == 3:
            str_output_arguments_composed = [f"{input_arguments_clean[1]['value']}, {input_arguments_clean[2]['value']}"]
    
        translated_function_call = " if" + function_name_formatting + "(" +  input_arguments_clean[0]['value'] +" "+ function_info["translation"]+" , " + ", ".join(str_output_arguments_composed) + ")"
        return f"{translated_function_call}"
    
    
    def round(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if len(input_arguments_clean) == 1:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
    
        elif len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']},{input_arguments_clean[1]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
    
    def dround(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if len(input_arguments_clean) == 1:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
    
        elif len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']},{input_arguments_clean[1]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
    
    def rand(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if len(input_arguments_clean) == 0:
            # str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" +  ")"
            return f"{translated_function_call}"
    
        elif len(input_arguments_clean) == 1:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
        
    def randome(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if len(input_arguments_clean) == 0:
            translated_function_call = function_info["translation"] + function_name_formatting + "(" +  ")"
            return f"{translated_function_call}"
    
        elif len(input_arguments_clean) == 1:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
        
    def trunc(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        presto_dict = {
                "day": "day",
                "dd": "day",
                "ddd": "day",
                "hour": "hour",
                "minute": "minute",
                "month": "month",
                "quarter": "quarter",
                "second": "second",
                "week": "week",
                "year": "year"
            }
        print(input_arguments_clean[1]['value'])           #ddd value is correct
        print(presto_dict[input_arguments_clean[1]['value']])              # throwing error
        translated_function_call = function_info["translation"] + function_name_formatting +"(" + presto_dict[input_arguments_clean[1]['value']] + "," + str_output_arguments_composed[0] + ")"
        return f"{translated_function_call}"
        
    def dtrunc(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if len(input_arguments_clean) == 1:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
    
        elif len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']},{input_arguments_clean[1]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
        
    def truncate(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        if len(input_arguments_clean) == 1:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"
    
        elif len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[0]['value']},{input_arguments_clean[1]['value']}"]
            translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
            return f"{translated_function_call}"

    def locate(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "isnull" function
        Example: isnull(1) --> 1 is null

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        if len(input_arguments_clean) == 2:
            str_output_arguments_composed = [f"{input_arguments_clean[1]['value']},{input_arguments_clean[0]['value']}"]
        translated_function_call = function_info["translation"] + function_name_formatting + "(" + ", ".join(str_output_arguments_composed) + ")"
        return f"{translated_function_call}"
    
    def months_between(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        
        # print(str_output_arguments_composed)
        translated_function_call ="(" + function_info["translation"] + function_name_formatting + "("+ ", ".join(str_output_arguments_composed) + ")" + "+1" + ")" + "/31.0"
        return f"{translated_function_call}"


    def extract(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:
        """Special translation considerations applicable the "extract" function
        Example: extract(day from '2020-03-25 16:32:01') --> extract(day from cast('2020-03-25 16:32:01' AS timestamp))

        Args:
            function_info (Dict): Entry in function_dictionaries
            function_name_formatting (str): String found between the function name & its parenthesis (most of the time empty but sometimes there is foo   (bar)...)
            input_arguments_clean (List[str]): List of the input arguments. Sometimes, one special function needs to look back at them.
            str_output_arguments_composed (List[str]): List of translated function arguments

        Returns:
            str: Translated function call
        """
        assert len(str_output_arguments_composed) == 3  # extract(a from b) ==> ['a', 'from', 'b']
        translated_function_call = "extract(" + str_output_arguments_composed[0]+" "+ str_output_arguments_composed[1]+" "+str_output_arguments_composed[2] + ")"
        return f"{translated_function_call}"
    
    def date_part(self, function_info: Dict, function_name_formatting: str, input_arguments_clean: List[str], str_output_arguments_composed: List[str]) -> str:

        translated_function_call = "extract(" + str_output_arguments_composed[0].replace("'","") + " from " + str_output_arguments_composed[1] + ")"
        return f"{translated_function_call}"

    def _translate_datetime_format(self, input_format: str) -> str:
        """Translates string identifiers used to indicate a timestamp/datetime format.
        This relies on the content of the datetime_parsing_dictionaries folder.
        Example: 'yyyy-MM-dd HH:mm:ss' --> '%Y-%m-%d %k:%i:%s'

        Args:
            input_format (str): Input datetime format string

        Returns:
            str: Translated datetime format string
        """
        def translation_helper(match):
            if match in self.datetime_translation["exact_matches"]:
                if not self.datetime_translation["exact_matches"][match].startswith("%"):
                    raise NotImplementedError
                return self.datetime_translation["exact_matches"][match]
            if self.datetime_translation["exact_matches"].get(match):  # Direct translation available
                return self.datetime_translation["exact_matches"][match]
            else:  # No direct translation. Try to sub parse it.
                if len(match) == 1:
                    raise NotImplementedError(f"No translation provided for '{match}' in '{input_format}'!")
                else:  # Sub parse
                    def sub_parsing_helper(match):  # The regex repeats group 2, but group 1 is extracted.
                        return self.datetime_translation["exact_matches"][match.group(1)]
                    # logging.debug(f"[DEBUG] Input to parse:{match}")
                    return Regex.sub(
                        r"((\w)\2*)",
                        lambda match: match.group(1) if match.group(1).isdigit() else sub_parsing_helper(match),
                        match,
                        strict=False
                    )

        return Regex.sub(
            r"(?P<token>\w+)",
            lambda match: translation_helper(match.group(1)),  # Translate if found in lookup table
            input_format
        )
