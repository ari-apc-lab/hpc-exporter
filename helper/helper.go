package helper

func ListContainsElement(s []string, str string) bool {
	for _, v := range s {
		if v == str {
			return true
		}
	}
	return false
}

func DeleteArrayEntry(s []string, str string) []string {
	var index int = -1
	var array []string = s
	for i, v := range s {
		if v == str {
			index = i
			break
		}
	}
	if index >= 0 {
		array = RemoveIndex(s, index)
	}
	return array
}

func RemoveIndex(s []string, index int) []string {
	return append(s[:index], s[index+1:]...)
}
