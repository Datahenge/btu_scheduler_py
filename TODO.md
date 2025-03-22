
# An unfinished idea
'''
pub fn cron_tz_to_cron_utc(cron_expression: &str, timezone: Tz) -> Result<Vec<String>, CronError> {
	"""
		Input: A timezone-specific Cron Expression.
		Output: A vector of UTC Cron Expression.

		Inspired and derived from: https://github.com/Sonic0/local-crontab ...
		... which itself was derived from https://github.com/capitalone/local-crontab created by United Income at Capital One.
	"""
	info!("Ok, will try to convert cron '{}' with time zone '{}' to a vector of UTC cron expressions.", cron_expression, timezone);

	let cron_struct: CronStruct = cron_expression.parse()?;

	// If the hour part of Cron is the entire range of hours (*), then not much to do.
	if cron_struct.hour.is_none() {
		return Ok(vec!(cron_struct.to_string()));
	}
	
	// Create the nested list with every single day belonging to the cron
	let utc_list_crontabs = _day_cron_list(cron_struct);
	// Group hours together
	utc_list_crontabs = _group_hours(utc_list_crontabs)
	// Group days together
	utc_list_crontabs = _group_days(utc_list_crontabs)
	// Convert a day-full month in *
	utc_list_crontabs = _range_to_full_month(utc_list_crontabs)
	// Group months together by hour / minute & days
	utc_list_crontabs = _group_months(utc_list_crontabs)

	let mut cron_strings: Vec<String> = Vec::new();
	for cron_list in utc_list_crontabs.iter() {
		let next_cron = CronStruct::from_integer_array(cron_list);
		let next_cron_string = next_cron.to_string();
		cron_strings.append(cron_str_to_cron_str7(next_cron_string));
	}
	Ok(cron_strings)
}
'''


# An unfinished idea
'''
type CronConverterNestedLists = Vec<Vec<Vec<u32>>>;

fn _day_cron_list(cron_struct: CronStruct) -> CronConverterNestedLists {
	/* 
		Returns a nested list struct in which each element represents every single day in cron list format,
		readable by Cron-Converter Object. Sometimes days included in the cron range do not exist in the real life for every month(example: February 30),
		so these days will be discarded.
		:return: acc (list of ints): nested list made up of cron lists readable by Cron-Converter Object.
	*/

	/*
	let utc_list_crontabs = Vec::new();
	for month in cron_struct.month {
		for day in cron_struct.day {
			for hour in self.localized_cron_list[1]:
				try:
					local_date = datetime(self.cron_year, month, day, hour, 0, tzinfo=self.timezone)
				except ValueError:
					continue  # skip days that not exist (eg: 30 February)
				utc_date = (local_date - local_date.utcoffset()).replace(tzinfo=timezone.utc)
				# Create one Cron list for each hour
				utc_list_crontabs.append([
					[minute for minute in self.localized_cron_list[0]],
					[utc_date.hour],
					[utc_date.day], [utc_date.month], self.localized_cron_list[4]])
		}
	}
	utc_list_crontabs
	*/	
}
'''

# An unfinished idea
'''
		# Get offset from utc in hours
		local_offset = self.timezone.utcoffset(datetime.now(self.timezone))
		local_offset_hours = int(local_offset.total_seconds() / 3600)  # offset in second / second in an hour

		utc_cron_list = self.localized_cron_list
		day_shift = (False, 0)
		hour_shifted_count = 0
		# Hours shift
		hour_range = self.localized_cron.parts[1].possible_values()  # Range of hours that a Cron hour object Part can assume
		cron_hours_part_utc = [hour - local_offset_hours for hour in self.localized_cron_list[1]]  # Shift hour based of offset from UTC
		for idx, hour in enumerate(cron_hours_part_utc):
			if hour < hour_range[0]:
				# Hour < 0 (ex: -2, -1) as intended in the previous day, so shift them to a real hour (ex: 22, 23)
				day_shift = (True, -1)
				hour += len(hour_range)  # Convert negative hour to real (ex: -2 + 24 = 22, -1 + 24 = 23)
				cron_hours_part_utc.pop(idx)
				cron_hours_part_utc.insert(idx, hour)
				hour_shifted_count += 1
			elif hour > hour_range[-1]:
				# Hour < 0 (ex: -2, -1) as intended in the previous day, so shift them to a real hour (ex: 22, 23)
				day_shift = (True, 1)
				hour -= len(hour_range)  # Convert not existing hour to real (ex: 25 - 24 = 1, 26 - 24 = 2)
				cron_hours_part_utc.pop(idx)
				cron_hours_part_utc.insert(idx, hour)
				hour_shifted_count += 1
		utc_cron_list[1] = cron_hours_part_utc

		# Day shift
		# if it is necessary a day shift and the original days Cron Part is not full(*)
		if day_shift[0] and not self.localized_cron.parts[2].is_full():
			# All hours shifted to the a next or previous day
			if day_shift[0] and hour_shifted_count == len(cron_hours_part_utc):
				utc_cron_list[2] = [day + day_shift[1] for day in self.localized_cron_list[2]]
			# Only one or more hours shifted to the a next or previous day
			elif day_shift[0] and hour_shifted_count != len(cron_hours_part_utc):
				raise ValueError("Operation cross days not supported. Sorry! (╥﹏╥)")

		utc_cron = Cron()
		utc_cron.from_list(utc_cron_list)

		return utc_cron.to_string()


	def _range_to_full_month(self, utc_list_crontabs: CronConverterNestedLists) -> CronConverterNestedLists:
		"""Returns a modified list with the character '*' as month in case of the month is day-full.
		The Cron-Converter read a full month only if it has 31 days.
		:return: acc (nested list of ints): modified nested list made up of cron lists readable by Cron-Converter Object.
		"""
		acc = []
		for element in utc_list_crontabs:
			if len(element[2]) == monthrange(self.cron_year, element[3][0])[1]:
				element[2] = [day for day in range(1, 32)]

			acc.append(element)
		return acc

	@staticmethod
	def _group_hours(utc_list_crontabs: CronConverterNestedLists) -> CronConverterNestedLists:
		"""Group hours together by minute, day and month.
		:param utc_list_crontabs: Nested list of crontabs not grouped.
		:return: acc (nested list of ints): filtered nested list made up of cron lists readable by Cron-Converter Object.
		"""
		acc = []
		for element in utc_list_crontabs:
			if len(acc) > 0 and \
					acc[-1][0] == element[0] and \
					acc[-1][2] == element[2] and \
					acc[-1][3] == element[3]:
				acc[-1][1].append(element[1][0])
			else:
				acc.append(element)
		return acc

	@staticmethod
	def _group_days(utc_list_crontabs: CronConverterNestedLists) -> CronConverterNestedLists:
		"""Group days together by hour, minute and month.
		:param utc_list_crontabs: Nested list of crontabs previously grouped in hours.
		:return: acc (nested list of ints): filtered nested list made up of cron lists readable by Cron-Converter Object.
		"""
		acc = []
		for element in utc_list_crontabs:
			if len(acc) > 0 and \
					acc[-1][0] == element[0] and \
					acc[-1][1] == element[1] and \
					acc[-1][3] == element[3]:
				acc[-1][2].append(element[2][0])
			else:
				acc.append(element)
		return acc

	@staticmethod
	def _group_months(utc_list_crontabs: CronConverterNestedLists) -> CronConverterNestedLists:
		"""Group months together by minute, days and hours
		:param utc_list_crontabs: Nested list of crontabs previously grouped in days.
		:return: acc (nested list of ints): filtered nested list made up of cron lists readable by Cron-Converter Object.
		"""
		acc = []
		for element in utc_list_crontabs:
			if len(acc) > 0 and \
					acc[-1][0] == element[0] and \
					acc[-1][1] == element[1] and \
					acc[-1][2] == element[2]:
				acc[-1][3].append(element[3][0])
			else:
				acc.append(element)
		return acc
'''

# An unfinished idea
def future_foo(cron_expression_string: str, _cron_timezone: ZoneInfo, _number_of_results: int):
	"""
	Concept
	#
	#	1. Take the Local Timezone cron expression string.
	#	2. Create a Struct instance from that.
	#	3. Based on this Local Cron, create a Vector of all possible UTC Cron Expressions.  There could be half a dozen.
	#	4. Loop through each UTC Cron Expression, and create the next N scheduled UTC datetimes.
	#	5. We now have M sets of N datetimes.
	#	6. Merge them, and eliminate uniques.
	#	7. Return the last of UTC Datetimes to the caller.  These are the next N run times.
	#
	"""

	'''
	match cron_str_to_cron_str7(cron_expression_string) {
		Ok(cron_string) => {

			// We now have a 7-element cron string.
			match Schedule::from_str(&cron_string) {
				Ok(_schedule) => {
					// Returns UTC Datetimes that are *after* the current UTC datetime now.
					// Unfortunately, UTC appears to be the only option.
					// return schedule.upcoming(Utc).take(10).next();
				},
				Err(error) => {
					error!("ERROR: Cannot parse invalid cron string: '{}'.  Error: {}", cron_string, error);
					// return None;
				}
			}
		},
		Err(error) => {
			error!("ERROR: Cannot parse invalid cron string: '{}'.  Error: {}", cron_expression_string, error);
			// return None;
	# end function 'future_foo'
	'''
