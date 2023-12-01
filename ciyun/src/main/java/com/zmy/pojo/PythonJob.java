package com.zmy.pojo;

public class PythonJob {

    public PythonJob() {
    }

    public PythonJob(String job_name, String company_name, String degree, String company_size, String area, String job_type, String salary, String exp, String time, String url) {
        this.job_name = job_name;
        this.company_name = company_name;
        this.degree = degree;
        this.company_size = company_size;
        this.area = area;
        this.job_type = job_type;
        this.salary = salary;
        this.exp = exp;
        this.time = time;
        this.url = url;
    }

    public PythonJob(String[] job_props) {
        this.job_name = job_props[0];
        this.company_name = job_props[1];
        this.degree = job_props[2];
        this.company_size = job_props[3];
        this.area = job_props[4];
        this.job_type = job_props[5];
        this.salary = job_props[6];
        this.exp = job_props[7];
        this.time = job_props[8];
        this.url = job_props[9];
    }


    private String job_name;

    private String company_name;

    private String degree;

    private String company_size;

    private String area;

    private String job_type;

    private String salary;

    private String exp;

    private String time;

    private String url;

    @Override
    public String toString() {
        return "PythonJob{" +
                "job_name='" + job_name + '\'' +
                ", degree='" + degree + '\'' +
                ", area='" + area + '\'' +
                ", job_type='" + job_type + '\'' +
                ", salary='" + salary + '\'' +
                '}';
    }

    public String getJob_name() {
        return job_name;
    }

    public void setJob_name(String job_name) {
        this.job_name = job_name;
    }

    public String getCompany_name() {
        return company_name;
    }

    public void setCompany_name(String company_name) {
        this.company_name = company_name;
    }

    public String getDegree() {
        return degree;
    }

    public void setDegree(String degree) {
        this.degree = degree;
    }

    public String getCompany_size() {
        return company_size;
    }

    public void setCompany_size(String company_size) {
        this.company_size = company_size;
    }

    public String getArea() {
        return area;
    }

    public void setArea(String area) {
        this.area = area;
    }

    public String getJob_type() {
        return job_type;
    }

    public void setJob_type(String job_type) {
        this.job_type = job_type;
    }

    public String getSalary() {
        return salary;
    }

    public void setSalary(String salary) {
        this.salary = salary;
    }

    public String getExp() {
        return exp;
    }

    public void setExp(String exp) {
        this.exp = exp;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}

