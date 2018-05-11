package com.matt;

public class HealthLog {
	//Member_ID,First_Name,Last_Name,Gender,Age,Height,Weight,Hours_Sleep,Calories_Consumed,Exercise_Calories_Burned,Date
	private String Member_ID;
	private String First_Name;
	private String Last_Name;
	private String Gender;
	private Integer Age;
	private Integer Height;
	private Integer Weight;
	private Integer Hours_Sleep;
	private Integer Calories_Consumed;
	private Integer Exercise_Calories_Burned;
	private String Date;
	
	public HealthLog(String member_ID, String first_Name, String last_Name, String gender, Integer age, Integer height,
			Integer weight, Integer hours_Sleep, Integer calories_Consumed, Integer exercise_Calories_Burned,
			String date) {
		super();
		Member_ID = member_ID;
		First_Name = first_Name;
		Last_Name = last_Name;
		Gender = gender;
		Age = age;
		Height = height;
		Weight = weight;
		Hours_Sleep = hours_Sleep;
		Calories_Consumed = calories_Consumed;
		Exercise_Calories_Burned = exercise_Calories_Burned;
		Date = date;
	}
	
	public String getMember_ID() {
		return Member_ID;
	}
	public void setMember_ID(String member_ID) {
		Member_ID = member_ID;
	}
	public String getFirst_Name() {
		return First_Name;
	}
	public void setFirst_Name(String first_Name) {
		First_Name = first_Name;
	}
	public String getLast_Name() {
		return Last_Name;
	}
	public void setLast_Name(String last_Name) {
		Last_Name = last_Name;
	}
	public String getGender() {
		return Gender;
	}
	public void setGender(String gender) {
		Gender = gender;
	}
	public Integer getAge() {
		return Age;
	}
	public void setAge(Integer age) {
		Age = age;
	}
	public Integer getHeight() {
		return Height;
	}
	public void setHeight(Integer height) {
		Height = height;
	}
	public Integer getWeight() {
		return Weight;
	}
	public void setWeight(Integer weight) {
		Weight = weight;
	}
	public Integer getHours_Sleep() {
		return Hours_Sleep;
	}
	public void setHours_Sleep(Integer hours_Sleep) {
		Hours_Sleep = hours_Sleep;
	}
	public Integer getCalories_Consumed() {
		return Calories_Consumed;
	}
	public void setCalories_Consumed(Integer calories_Consumed) {
		Calories_Consumed = calories_Consumed;
	}
	public Integer getExercise_Calories_Burned() {
		return Exercise_Calories_Burned;
	}
	public void setExercise_Calories_Burned(Integer exercise_Calories_Burned) {
		Exercise_Calories_Burned = exercise_Calories_Burned;
	}
	public String getDate() {
		return Date;
	}
	public void setDate(String date) {
		Date = date;
	}
	

}
