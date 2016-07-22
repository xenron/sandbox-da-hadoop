package com.ch6;

import java.util.List;
import java.util.Map;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.Query;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.impetus.client.hbase.HBaseClient;
import com.impetus.kundera.client.Client;

public class EmployeeTest {

	private static EntityManagerFactory emf = Persistence
			.createEntityManagerFactory("hbaseTest");
	private static EntityManager em = emf.createEntityManager();

	public static void main(String[] args) {

		Employee employee = new Employee();
		employee.setEmployeeId("1");
		employee.setEmployeeName("John");
		employee.setAddress("Atlanta");
		employee.setDepartment("R&D Labs");

		// persist employee record.
		em.persist(employee);

		System.out.println("Finding employee record operation.");

		// Find persisted employee record.
		Employee foundEmployee = em.find(Employee.class, "1");

		System.out.println(foundEmployee.getEmployeeId());
		System.out.println(foundEmployee.getEmployeeName());
		System.out.println(foundEmployee.getAddress());
		System.out.println(foundEmployee.getDepartment());

		// update employee record.
		System.out.println("Updating existing employee record.");
		foundEmployee.setAddress("New York");
		em.merge(foundEmployee);

		System.out.println("Finding employee post merge operation.");

		// Find updated employee record.
		Employee updatedEmployee = em.find(Employee.class, "1");

		System.out.println(updatedEmployee.getEmployeeId());
		System.out.println(updatedEmployee.getEmployeeName());
		System.out.println(updatedEmployee.getAddress());
		System.out.println(updatedEmployee.getDepartment());

		// delete employee record.
		em.remove(foundEmployee);

		// Find deleted employee record, it should be null.
		Employee deletedEmployee = em.find(Employee.class, "1");

		System.out.println("After deletion employee object is"
				+ deletedEmployee);

		query();

		queryWithFilter();
		// close em instance.
		em.close();

		// close emf instance.
		emf.close();
	}

	public static void query() {

		Employee employee = new Employee();
		employee.setEmployeeId("1");
		employee.setEmployeeName("John");
		employee.setAddress("Atlanta");
		employee.setDepartment("R&D Labs");

		// persist employee record.
		em.persist(employee);

		//Select without where cause 1.e. Select all. 
		String queryString = "Select e from Employee e";
		Query query = em.createQuery(queryString);
		List<Employee> employees = query.getResultList();

		System.out.println(employees.size());

		employee = employees.get(0);

		System.out.println(employee.getEmployeeId());
		System.out.println(employee.getEmployeeName());
		System.out.println(employee.getAddress());
		System.out.println(employee.getDepartment());

		// Select Query with constraints on employee name.
		queryString = "Select e from Employee e where e.employeeName=John";
		query = em.createQuery(queryString);
		employees = query.getResultList();

		System.out.println(employees.size());

		employee = employees.get(0);

		System.out.println(employee.getEmployeeId());
		System.out.println(employee.getEmployeeName());
		System.out.println(employee.getAddress());
		System.out.println(employee.getDepartment());

		// Select specific columns only.
		queryString = "Select e.employeeName, e.address from Employee e";
		query = em.createQuery(queryString);
		employees = query.getResultList();

		System.out.println(employees.size());

		employee = employees.get(0);

		System.out.println(employee.getEmployeeId()); 
		System.out.println(employee.getEmployeeName());
		System.out.println(employee.getAddress());
		System.out.println(employee.getDepartment()); 
	}

	public static void queryWithFilter() {

		Employee employee = new Employee();
		employee.setEmployeeId("2");
		employee.setEmployeeName("Anand");
		employee.setAddress("Atlanta");
		employee.setDepartment("R&D Labs");

		// persist employee record.
		em.persist(employee);

		Map<String, Client> clients = (Map<String, Client>) em.getDelegate();

		HBaseClient client = (HBaseClient) clients.get("hbaseTest");

		Filter prefixFilter = new PrefixFilter(Bytes.toBytes("1"));

		client.setFilter(prefixFilter);

		String queryString = "Select e from Employee e";
		Query query = em.createQuery(queryString);
		List<Employee> employees = query.getResultList();

		System.out.println(employees.size());

		employee = employees.get(0);

		System.out.println(employee.getEmployeeId());
		System.out.println(employee.getEmployeeName());
		System.out.println(employee.getAddress());
		System.out.println(employee.getDepartment());

		Filter keyOnlyFilter = new KeyOnlyFilter();

		// Only row key will be fetched.
		client.setFilter(keyOnlyFilter); 
		queryString = "Select e from Employee e";
		query = em.createQuery(queryString);
		employees = query.getResultList();

		System.out.println(employees.size());

		employee = employees.get(0);

		System.out.println(employee.getEmployeeId());
		System.out.println(employee.getEmployeeName());
														
		System.out.println(employee.getAddress());
		System.out.println(employee.getDepartment());
	}
}
